use criterion::{criterion_group, criterion_main, Bencher, Criterion};

use pb_file_reader::notify::GLOBAL_LISTENER;
use pb_file_reader::utils::model::*;

fn bench_notify(c: &mut Criterion) {
    let file_num = 20;
    let mut notify_metas = vec![];

    for i in 1..file_num {
        let file_name = format!("/Users/yaohui/projects/pb_file_reader/test/{}.csv", i);
        match GLOBAL_LISTENER.add_watch(std::path::PathBuf::from(file_name)) {
            Ok(meta) => {
                println!("add watch success");
                notify_metas.push(meta);
            }
            Err(e) => {
                println!("err:{:?}", e)
            }
        }
    }

    use std::time::Duration;

    c.bench_function("notify_benchmark", |b| {
        b.iter(|| {
            for _ in 0..10000 {
                let receivers: Vec<&crossbeam::channel::Receiver<NotifyEventData>> =
                    notify_metas.iter().map(|meta| &meta.receiver).collect();

                let mut sel = crossbeam::channel::Select::new();
                for receiver in &receivers {
                    sel.recv(receiver);
                }

                // 使用 try_select 避免 SelectedOperation 生命周期问题
                if let Ok(oper) = sel.try_select() {
                    let index = oper.index();
                    println!("Received data from channel {}", index);
                }
            }
        });
    });

}

criterion_group!(benches, bench_notify);
criterion_main!(benches);