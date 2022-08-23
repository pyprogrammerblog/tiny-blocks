.. tiny-blocks documentation master file, created by
   sphinx-quickstart on Tue Aug  9 09:38:22 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
.. _index:

Welcome to tiny-blocks' documentation!
=======================================

Tiny Blocks to build large and complex ETL pipelines!

Tiny-Blocks is a library for **data engineering** operations.
Each **pipeline** is made out of **tiny-blocks** glued with the `>>` operator.
This library relies on a fundamental streaming abstraction consisting of three
parts: **extract**, **transform**, and **load**. You can view a pipeline
as an extraction, followed by zero or more transformations, followed by a sink.
Visually, this looks like::

   extract -> transform1 -> transform2 -> ... -> transformN -> load


You can also `fan-in`, `fan-out` or `tee` for more complex operations::

   extract1 -> transform1 -> |-> transform2 -> ... -> | -> transformN -> load1
   extract2 ---------------> |                        | -> load2


Tiny-Blocks use **generators** to stream data. Each **chunk** is a **Pandas DataFrame**.
The `chunksize` or buffer size is adjustable per pipeline.


Basic usage
------------

Make sure you had install the package by doing ``pip install tiny-blocks`` and then::

   from tiny_blocks.extract import FromCSV
   from tiny_blocks.transform import Fillna
   from tiny_blocks.load import ToSQL

   # ETL Blocks
   from_csv = FromCSV(path='/path/to/source.csv')
   fill_na = Fillna(value="Hola Mundo")
   to_sql = ToSQL(dsn_conn='psycopg2+postgres://...', table_name="sink")

   # Pipeline
   from_csv >> fill_na >> to_sql

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   tiny-blocks
   extract
   transform
   load
   license
   help


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
