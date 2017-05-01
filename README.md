# Spark Mapper

[![Build status](https://api.travis-ci.org/log0ymxm/spark-mapper.svg?branch=master)](https://travis-ci.org/log0ymxm/spark-mapper)
[![codecov](https://codecov.io/gh/log0ymxm/spark-mapper/branch/master/graph/badge.svg)](https://codecov.io/gh/log0ymxm/spark-mapper)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.log0ymxm/spark-mapper_2.11.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.log0ymxm%22%20AND%20a%3A%22spark-mapper_2.11%22)

Mapper is a topological data anlysis technique for estimating a lower dimensional simplicial complex from a dataset. It was initially described in the paper "Topological Methods for the Analysis of High Dimensional Data Sets and 3D Object Recognition." [1]

Concentric Circles         |  MNIST Twos
:-------------------------:|:-------------------------:
![Concentric circles](https://github.com/log0ymxm/spark-mapper/raw/master/examples/concentric_circles.png)  |  ![MNIST](https://github.com/log0ymxm/spark-mapper/raw/master/examples/mnist_twos.png)

# Things to do

- [ ] Improve the handling of pairwise distances. This is likely the largest bottleneck for large datasets.
- [ ] Implement some useful filter functions: Gaussian Density, Graph Laplacian, etc
- [ ] Implement different methods for choosing cluster cutoff. There's a few simple ones we can try, and the scale graph idea. 
- [ ] Explore using a distributed clustering algorithm. Currently clustering is local for each cover segment, which means that as data grows you need to increase the cover intervals proportionally to keep the partitions within memory. A distributed cluster would remove this requirement.

# Related Software

- [Python Mapper](http://danifold.net/mapper/index.html)
- [TDA Mapper (R)](https://github.com/paultpearson/TDAmapper/)

# References

1. G. Singh, F. Memoli, G. Carlsson (2007). Topological Methods for the Analysis of High Dimensional Data Sets and 3D Object Recognition, Point Based Graphics 2007, Prague, September 2007.
2. Daniel Müllner and Aravindakshan Babu, Python Mapper: An open-source toolchain for data exploration, analysis and visualization, 2013, URL http://danifold.net/mapper
