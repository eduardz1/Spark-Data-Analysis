#import "@preview/codly:1.1.1": *
#import "@preview/codly-languages:0.1.3": *

#let eqcolumns(n, gutter: 4%, content) = {
  layout(size => [
    #let (height,) = measure(
      block(
        width: (1 / n) * size.width * (1 - float(gutter) * n),
        content,
      ),
    )
    #block(
      height: height / n,
      columns(n, gutter: gutter, content),
    )
  ])
}

#let template(
  title: [],
  subtitle: [],
  authors: (),
  lang: "en",
  bibliography-file: "works.bib",
  body,
) = {
  set document(title: title, author: authors)
  set text(font: "New Computer Modern", size: 10pt, lang: lang)
  set page(paper: "a4")
  set par(justify: true, first-line-indent: 1.8em)

  show figure.caption: emph

  set heading(numbering: "1.1")
  show heading: smallcaps
  show heading: set block(above: 1.4em, below: 1em)
  set align(horizon)

  set outline(fill: repeat[ #sym.space #sym.dot.c ], indent: true)
  show outline.entry.where(level: 1): it => {
    v(1.2em, weak: true)
    strong(it)
  }

  show table.cell.where(y: 0): strong
  set table(
    stroke: (x: none, y: 0.5pt + black),
    row-gutter: (2.2pt, auto),
    align: (x, y) => (
      if x > 0 {
        center
      } else {
        left
      }
    ),
  )

  show raw: set text(font: "Fira Code")
  show raw.where(block: true): set text(size: 0.8em)
  show: codly-init
  codly(
    languages: codly-languages,
    zebra-fill: none,
    number-format: it => text(fill: luma(200), str(it)),
  )

  {
    // Title Page
    set align(center)
    set page(footer: text(fill: gray)[ #subtitle \ #datetime.today().display()])
    let width = 70%

    v(0.4fr)

    // image("imgs/logo.png", width: width)
    line(length: width, stroke: 4pt)
    block(
      smallcaps(
        text(
          size: 3em,
          title,
        ),
      ),
    )
    line(length: width)

    // Author and Academic Year
    box(
      width: width,
      grid(columns: authors.len(), column-gutter: 1fr, ..authors),
    )

    v(1fr)

    outline()

    v(1fr)
  }

  pagebreak()

  set page(numbering: "1")

  body

  // pagebreak()
  // bibliography(bibliography-file)
}
