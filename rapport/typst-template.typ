#let rapport_V1(
  title: "title",
  body,
) = {

 set text(
    font: "Niramit",
    size: 12pt,
  )

 set page(
    "us-letter",
    margin: (left: 1in, right: 1in, top: 0.7in, bottom: 1in),
    background: place(top, rect(fill: rgb("dc3230"), width: 100%, height: 0.5in)),
    header: align(
      horizon,
      grid(
        columns: (80%, 20%),
        align(left, text(size: 20pt, fill: white, weight: "bold", title))
      ),
    )
  )
  
  set par(
  justify: true
  )
  body
}