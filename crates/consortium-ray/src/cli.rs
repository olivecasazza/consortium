//! Lines printed by Ray's `cli_logger`. Without a TTY each line is a record,
//! `<time>\t<LEVEL> cli.py:<line> -- <message>` ([record format]
//! [cli_logger rendering]), and the message may still be wrapped in one ANSI
//! color.
//!
//! [record format]: https://github.com/ray-project/ray/blob/ray-2.48.0/python/ray/_private/ray_constants.py#L235
//! [cli_logger rendering]: https://github.com/ray-project/ray/blob/ray-2.48.0/python/ray/autoscaler/_private/cli_logger.py#L369-L422

/// The message of a `cli.py` record logged at `level`, or of a plain (TTY) line.
///
/// Records from other files, such as job logs quoted in a status message, are
/// ignored. Only one outer color is removed: escape codes inside the text
/// remain, so splicing colored fragments cannot invent an identifier.
pub(crate) fn message<'a>(line: &'a str, level: &str) -> Option<&'a str> {
    let mut message = line;
    if let Some((time, record)) = line.split_once('\t') {
        let (location, text) = record
            .strip_prefix(level)?
            .strip_prefix(" cli.py:")?
            .split_once(" -- ")?;
        const TIMESTAMP: &[u8] = b"####-##-## ##:##:##,###";
        if time.len() != TIMESTAMP.len()
            || !time.bytes().zip(TIMESTAMP).all(|(c, &field)| match field {
                b'#' => c.is_ascii_digit(),
                _ => c == field,
            })
            || !decimal(location)
        {
            return None;
        }
        message = text;
    }
    if let Some(colored) = message.strip_prefix("\x1b[") {
        let (style, text) = colored.split_once('m')?;
        if !style.bytes().all(|b| b.is_ascii_digit() || b == b';') {
            return None;
        }
        message = text
            .strip_suffix("\x1b[39m")
            .or_else(|| text.strip_suffix("\x1b[0m"))?;
    }
    Some(message)
}

pub(crate) fn decimal(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit())
}
