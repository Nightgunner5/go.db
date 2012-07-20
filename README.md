go.db [![Build Status](https://secure.travis-ci.org/Nightgunner5/go.db.png?branch=master)](http://travis-ci.org/Nightgunner5/go.db)
=====

Now using B+ trees for epic speedups!


TODO:
-----

- Implement Delete
- Implement Update
- Implement more index types
- Library interface
- `database/sql/driver` interface

Testimonials
------------

    <Nightgunner5> BenchmarkQuery        50          46427740 ns/op           0.55 MB/s
    <Nightgunner5> My database is so efficient
    <zeebo> blazing speed

    <Nightgunner5> Well, I think I made my database implementation slightly better: Old 41333680 ns/op New 24179 ns/op
    <nsf> in that case 100 bytes per action doesn't seem like a big deal
    <nsf> Nightgunner5: :D

