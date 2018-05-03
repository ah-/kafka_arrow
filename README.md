Bulk consume kafka topic in Python via Apache Arrow

```
python setup.py build_ext --inplace
python example.py
```

### dependencies
You will need to make sure the following Operating System and Python tools and libraries are available. 
#### OS
1. g++ (c++ compiler, _g++_ on linux, _clang_ on Mac)

```
  sudo apt-get install g++            # or equivalent for your platform
```

2. arrow-dev (c/c++ arrow C bindings. libs and headers).
Install by following instructions or [https://arrow.apache.org/install/](https://arrow.apache.org/install/)

3. jansson (JSON parser)
```
  sudo apt-get install libjansson-dev  # or equivalent for your platform
```

4. avro 
```
  sudo apt-get install libavro-dev  # or equivalent for your platform
```

5. serdes
```
  cd .../libserdes
  ./configure
  make
  make install
```

6. librdkafka (Kafka C client)
```
  sudo apt-get install librdkafka-dev  # or equivalent for your platform
```


#### Python
1. Cython
2. numpy
3. pyarrow
