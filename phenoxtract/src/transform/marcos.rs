#[macro_export]
macro_rules! collect_contexts {
    ($( $opt:expr => $ctx:expr ),+ $(,)?) => {{
        std::iter::empty()
        $(
            .chain(
                $opt.as_ref()
                    .map(|_| $ctx)
                    .into_iter()
                    .flatten()
            )
        )+
        .collect::<Vec<_>>()
    }};
}
