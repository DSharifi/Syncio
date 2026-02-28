fn test_function(foo: tokio::runtime::Handle) {
    foo.spawn(async { println!("test future to check if CI cache works") });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_test_function() {
        test_function(tokio::runtime::Handle::current());
    }
}
