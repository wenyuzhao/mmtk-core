use atomic::Ordering;
use prost::Message;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

use crate::plan::HasSpaces;
use crate::vm::slot::Slot;
use crate::vm::ObjectModel;
use crate::vm::Scanning;
use crate::vm::VMBinding;

use super::address::{CLDScanPolicy, RefScanPolicy};
use super::Address;
use super::ObjectReference;

static GC_EPOCH: AtomicUsize = AtomicUsize::new(0);

lazy_static! {
    static ref SHAPES_ITER: Mutex<heapdump::ShapesIteration> =
        Mutex::new(heapdump::ShapesIteration { epochs: vec![] });
    static ref HEAPDUMP: Mutex<heapdump::HeapDump> = Mutex::new(heapdump::HeapDump::new());
}

mod heapdump {
    include!(concat!(env!("OUT_DIR"), "/mmtk.util.sanity.rs"));
}

impl heapdump::HeapDump {
    fn dump_to_file(&self, path: impl AsRef<Path>) {
        let file = File::create(path).unwrap();
        let mut writer = zstd::Encoder::new(file, 0).unwrap().auto_finish();
        let mut buf = Vec::new();
        self.encode(&mut buf).unwrap();
        writer.write_all(&buf).unwrap();
    }

    fn reset(&mut self) {
        self.objects.clear();
        self.roots.clear();
        self.spaces.clear();
    }

    fn new() -> Self {
        heapdump::HeapDump {
            objects: vec![],
            roots: vec![],
            spaces: vec![],
        }
    }
}

fn obj_value(obj: ObjectReference) -> u64 {
    obj.to_raw_address().as_usize() as u64
}

pub fn record<VM: VMBinding>(roots: bool, new_object: ObjectReference) {
    if !crate::inside_harness() {
        return;
    }
    let mut heapdump = HEAPDUMP.lock().unwrap();

    if roots {
        heapdump.roots.push(heapdump::RootEdge {
            objref: obj_value(new_object),
        });
    }

    let mut shapes_iter = SHAPES_ITER.lock().unwrap();

    let mut edges: Vec<heapdump::NormalEdge> = vec![];
    let mut objarray_length: Option<u64> = None;
    let mut instance_mirror_start: Option<u64> = None;
    let mut instance_mirror_count: Option<u64> = None;

    if <VM as VMBinding>::VMScanning::is_val_array(new_object) {
        shapes_iter
            .epochs
            .last_mut()
            .unwrap()
            .shapes
            .push(heapdump::Shape {
                kind: heapdump::shape::Kind::ValArray as i32,
                object: obj_value(new_object),
                offsets: vec![],
            });
    } else if <VM as VMBinding>::VMScanning::is_obj_array(new_object) {
        shapes_iter
            .epochs
            .last_mut()
            .unwrap()
            .shapes
            .push(heapdump::Shape {
                kind: heapdump::shape::Kind::ObjArray as i32,
                object: obj_value(new_object),
                offsets: vec![],
            });

        new_object.iterate_fields::<VM, _>(
            CLDScanPolicy::Ignore,
            RefScanPolicy::Follow,
            |e, ooh| {
                if ooh {
                    return;
                }
                edges.push(heapdump::NormalEdge {
                    slot: e.to_address().as_usize() as u64,
                    objref: e.load().map(obj_value).unwrap_or(0),
                });
            },
        );

        objarray_length = Some(edges.len() as u64);
    } else {
        if let Some((mirror_start, mirror_count)) =
            <VM as VMBinding>::VMScanning::instance_mirror_info(new_object)
        {
            instance_mirror_start = Some(mirror_start);
            instance_mirror_count = Some(mirror_count);
        }
        let mut s = vec![];
        new_object.iterate_fields::<VM, _>(
            CLDScanPolicy::Ignore,
            RefScanPolicy::Follow,
            |e, ooh| {
                if ooh {
                    return;
                }
                s.push(e.to_address().as_usize() as i64 - obj_value(new_object) as i64);
            },
        );
        // if s.len() > 512 {
        //     <VM as VMBinding>::VMObjectModel::dump_object(object);
        // }
        shapes_iter
            .epochs
            .last_mut()
            .unwrap()
            .shapes
            .push(heapdump::Shape {
                kind: heapdump::shape::Kind::Scalar as i32,
                object: obj_value(new_object),
                offsets: s,
            });
        new_object.iterate_fields::<VM, _>(
            CLDScanPolicy::Ignore,
            RefScanPolicy::Follow,
            |e, ooh| {
                if ooh {
                    return;
                }
                edges.push(heapdump::NormalEdge {
                    slot: e.to_address().as_usize() as u64,
                    objref: e.load().map(obj_value).unwrap_or(0),
                });
            },
        );
    }

    heapdump.objects.push(heapdump::HeapObject {
        start: obj_value(new_object),
        klass: new_object.class_pointer::<VM>().as_usize() as u64,
        size: <VM as VMBinding>::VMObjectModel::get_current_size(new_object) as u64,
        objarray_length,
        instance_mirror_start,
        instance_mirror_count,
        edges: edges,
    })
}

pub fn prepare() {
    if !crate::inside_harness() {
        return;
    }
    SHAPES_ITER
        .lock()
        .unwrap()
        .epochs
        .push(heapdump::ShapesEpoch { shapes: vec![] });
}

pub fn dump<VM: VMBinding>(plan: &impl HasSpaces<VM = VM>) {
    if !crate::inside_harness() {
        return;
    }
    let gc_count = GC_EPOCH.fetch_add(1, Ordering::SeqCst);
    plan.for_each_space(&mut |s| {
        let common = s.common();
        assert!(
            common.contiguous,
            "Only support heapdump of contiguous spaces"
        );
        HEAPDUMP.lock().unwrap().spaces.push(heapdump::Space {
            name: common.name.to_owned(),
            start: common.start.as_usize() as u64,
            end: (common.start + common.extent).as_usize() as u64,
        })
    });
    // Fix edges
    {
        let mut heapdump = HEAPDUMP.lock().unwrap();
        for o in heapdump.objects.iter_mut() {
            for e in o.edges.iter_mut() {
                let t = unsafe { *(e.slot as *mut u64) };
                if let Some(_fwd) =
                    ObjectReference::from_raw_address(unsafe { Address::from_usize(t as usize) })
                        .map(|x| x.get_forwarded_object::<VM>())
                        .flatten()
                {
                    unreachable!()
                } else {
                    e.objref = t;
                }
            }
        }
    }
    // Drop dead refs
    {
        let mut heapdump = HEAPDUMP.lock().unwrap();

        let mut objects: HashMap<_, _> = HashMap::new();
        let mut live_objects = vec![];
        for object in &heapdump.objects {
            objects.insert(object.start, object.clone());
        }
        let mut reachable_objects: HashSet<u64> = HashSet::new();
        let mut mark_stack: Vec<u64> = vec![];
        for root in &heapdump.roots {
            assert!(objects.contains_key(&root.objref));
            mark_stack.push(root.objref);
        }
        while let Some(o) = mark_stack.pop() {
            if reachable_objects.contains(&o) {
                continue;
            }
            reachable_objects.insert(o);
            if !objects.contains_key(&o) {
                panic!("Object not found: 0x{:x} ", o);
            }
            let obj = objects.get(&o).unwrap();
            live_objects.push(obj.clone());
            for edge in &obj.edges {
                let o = edge.objref;
                if o != 0 {
                    mark_stack.push(o);
                }
            }
        }

        heapdump.objects = live_objects;

        assert_eq!(heapdump.objects.len(), reachable_objects.len());
    }
    std::fs::create_dir_all("scratch/_heapdump").unwrap();
    HEAPDUMP
        .lock()
        .unwrap()
        .dump_to_file(format!("scratch/_heapdump/heapdump.{}.binpb.zst", gc_count));

    HEAPDUMP.lock().unwrap().reset();
}
