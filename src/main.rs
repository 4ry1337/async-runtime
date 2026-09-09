use std::time::Duration;

struct CounterFuture {
    count: u32,
}

impl Future for CounterFuture {
    type Output = u32;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.count = self.count.wrapping_add(1);
        println!("polling with result: {}", self.count);
        std::thread::sleep(Duration::from_secs(1));
        if self.count < 5 {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(self.count)
        }
    }
}

#[tokio::main]
async fn main() {
    let counter1 = CounterFuture { count: 0 };
    let counter2 = CounterFuture { count: 0 };
    let handle1 = tokio::task::spawn(async move { counter1.await });
    let handle2 = tokio::task::spawn(async move { counter2.await });
    tokio::join!(handle1, handle2);
}
