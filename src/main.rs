use std::{
    io,
    panic::catch_unwind,
    pin::Pin,
    sync::LazyLock,
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};

use async_task::{Runnable, Task};
use flume::{Receiver, Sender};
use futures_lite::future;

macro_rules! spawn_task {
    ($future:expr) => {
        spawn_task!($future, FutureType::Low)
    };
    ($future:expr, $order:expr) => {
        spawn_task($future, $order)
    };
}

macro_rules! join {
    ($($future:expr), *) => {
        {
            let mut results = Vec::new();
            $(
                results.push(future::block_on($future));
            )*
            results
        }
    };
}

macro_rules! try_join {
    ($($future:expr), *) => {
        {
            let mut results = Vec::new();
            $(
                let result = catch_unwind(|| future::block_on($future));
                results.push(result);
            )*
            results
        }
    };
}

#[derive(Debug, Clone, Copy)]
enum FutureType {
    High,
    Low,
}

fn spawn_task<F, T>(future: F, order: FutureType) -> Task<T>
//TODO: why 'static in detail?
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let schedule_high = |runnable| match HIGH_QUEUE.send(runnable) {
        Ok(result) => result,
        Err(err) => {
            println!("{err}");
        }
    };
    let schedule_low = |runnable| match LOW_QUEUE.send(runnable) {
        Ok(result) => result,
        Err(err) => {
            println!("{err}");
        }
    };
    let schedule = match order {
        FutureType::High => schedule_high,
        FutureType::Low => schedule_low,
    };
    let (runnable, task) = async_task::spawn(future, schedule);
    runnable.schedule();
    task
}

pub struct Runtime {
    pub high_num: usize,
    pub low_num:  usize,
}

impl Runtime {
    /// # Errors
    ///
    /// Returns an error if the number of available cores can't be determined.
    pub fn new() -> io::Result<Self> {
        let num_cores = std::thread::available_parallelism()?.get();

        Ok(Self {
            high_num: std::cmp::max(num_cores.saturating_sub(2), 1),
            low_num:  1,
        })
    }

    #[must_use]
    pub const fn with_high_num(mut self, high_num: usize) -> Self {
        self.high_num = high_num;
        self
    }

    #[must_use]
    pub const fn with_low_num(mut self, low_num: usize) -> Self {
        self.low_num = low_num;
        self
    }

    pub fn run(&self) {
        unsafe {
            std::env::set_var("HIGH_NUM", self.high_num.to_string());
            std::env::set_var("LOW_NUM", self.low_num.to_string());
        }
        let high = spawn_task!(async {}, FutureType::High);
        let low = spawn_task!(async {}, FutureType::Low);
        join!(high, low);
    }
}

static HIGH_QUEUE: LazyLock<flume::Sender<Runnable>> = LazyLock::new(|| {
    #[allow(clippy::expect_used)]
    let high_num = std::env::var("HIGH_NUM")
        .expect("HIGH_NUM is not set")
        .parse::<usize>()
        .expect("HIGH_NUM is not a valid number");

    for index in 0..high_num {
        let high_receiver = HIGH_CHANNEL.1.clone();
        let low_receiver = LOW_CHANNEL.1.clone();
        thread::spawn(move || {
            loop {
                match high_receiver.try_recv() {
                    Ok(runnable) => {
                        println!("[high thread {index}] running task from high queue");
                        let _ = catch_unwind(|| runnable.run());
                    }
                    Err(_) => match low_receiver.try_recv() {
                        Ok(runnable) => {
                            println!("[high thread {index}] running task from low queue");
                            let _ = catch_unwind(|| runnable.run());
                        }
                        Err(_) => {
                            thread::sleep(Duration::from_millis(100));
                        }
                    },
                }
            }
        });
    }
    HIGH_CHANNEL.0.clone()
});

static LOW_QUEUE: LazyLock<flume::Sender<Runnable>> = LazyLock::new(|| {
    #[allow(clippy::expect_used)]
    let low_num = std::env::var("LOW_NUM")
        .expect("LOW_NUM is not set")
        .parse::<usize>()
        .expect("LOW_NUM is not a valid number");

    for index in 0..low_num {
        let receiver = LOW_CHANNEL.1.clone();
        thread::spawn(move || {
            while let Ok(runnable) = receiver.recv() {
                println!("[low thread {index}] running task from low queue");
                let _ = catch_unwind(|| runnable.run());
            }
        });
    }

    LOW_CHANNEL.0.clone()
});

static HIGH_CHANNEL: LazyLock<(Sender<Runnable>, Receiver<Runnable>)> =
    LazyLock::new(flume::unbounded::<Runnable>);

static LOW_CHANNEL: LazyLock<(Sender<Runnable>, Receiver<Runnable>)> =
    LazyLock::new(flume::unbounded::<Runnable>);

fn main() {
    #[allow(clippy::expect_used)]
    Runtime::new()
        .expect("RUNTIME ERROR")
        .with_low_num(2)
        .with_high_num(4)
        .run();
    let one = CounterFuture { count: 0 };
    let two = CounterFuture { count: 0 };
    let t_one = spawn_task(one, FutureType::High);
    let t_two = spawn_task(two, FutureType::Low);
    let t_three = spawn_task!(async_fn());
    let t_four = spawn_task!(
        async {
            async_fn().await;
            async_fn().await;
        },
        FutureType::High
    );
    thread::sleep(Duration::from_secs(5));
    println!("before the block");
    let outcome: Vec<u32> = join!(t_one, t_two);
    let outcome_two: Vec<()> = join!(t_three, t_four);
}

struct AsyncSleep {
    start_time: Instant,
    duration:   Duration,
}

impl AsyncSleep {
    fn new(duration: Duration) -> Self {
        Self {
            start_time: Instant::now(),
            duration,
        }
    }
}

impl Future for AsyncSleep {
    type Output = bool;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let elapsed_time = self.start_time.elapsed();
        if elapsed_time >= self.duration {
            Poll::Ready(true)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

struct CounterFuture {
    count: u32,
}

impl Future for CounterFuture {
    type Output = u32;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.count = self.count.wrapping_add(1);
        println!("polling with result {}", self.count);
        thread::sleep(Duration::from_secs(1));
        if self.count < 3 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(self.count)
        }
    }
}

#[allow(clippy::unused_async)]
async fn async_fn() {
    thread::sleep(Duration::from_secs(1));
    println!("async fn");
}
