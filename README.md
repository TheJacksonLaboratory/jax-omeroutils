# jax-omeroutils

A package for working with OMERO by ResearchIT at The Jackson Laboratory.

This repository also contains scripts that use jax-omeroutils for managing commons tasks.


## A note on plates

Spreadsheet submission forms need to **either** contain columns named "dataset" and "project", **or** a column named "screen". Plates should be submitted with a "screen" column, and **only one plate per folder is allowed** (this is due to limitations on session time server-side). If multiple plates need to be submitted, they should be put in **separate folders** with **separate submission forms**.

When submitting plates, here is how this workflow behaves for different situations:

- Multiple plates in the same submission folder: the first plate will import correctly and be correctly put into the relevant screen and annotated; following plates will import correctly, but will NOT be moved to the right screen or annotated, no matter what the contents of the submission form are.
- Plate and regular images in the same submission folder, submission spreadsheet contains "project" and "dataset" columns: all will be imported correctly, but plate will not be placed in the project/dataset nor be annotated.
- Plate and regular images in the same submission folder, submission spreadsheet contains "screen" column: all be imported correctly, but regular images will not be placed in screen nor be annotated.
