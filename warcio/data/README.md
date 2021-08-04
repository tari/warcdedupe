The files in this directory are read by `build.rs` in order to generate
code to do simple mappings between standard values in a WARC file and the
Rust enum variants that represent them.

This data uses code generation rather than any kind of macro-generation
approach because it uses a PHF map of `uncased` strings which are not
supported by the `phf_map!` macro- thus it must use `phf_codegen` to
generate the string-to-variant mapping.

Given an enum like this:

```rust
enum Foo {
    Bar,
    Baz,
    Help(String),
}
```

ini files with the following structure are consumed:

```ini
; the name of the source file to be written into $OUT_DIR
file = foo_type.rs
; the name of the type to generate conversions for
type = Foo
; the name of a variant that contains a String, matching any unspecified value
catchall = Help

; A mapping from variant identifier to canonical string representation
; of that variant.
[values]
Bar = "Hello, world!"
Baz = baz
```

The generated code provides an implementation of `std::convert::From` that
takes string-like values and returns an enum value (`Foo` in this example),
and implements `AsRef<str>` for the enum type, mapping each variant
back to its canonical string representation (or the contained string for
the catchall variant).
