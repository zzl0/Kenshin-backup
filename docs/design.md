# Design Decision

## Interface

首先，我们来看一下已有的指标系统的数据接口。


### Write

``` python
# influxdb

data = [
  {"points":[[1.1,4.3,2.1],[1.2,2.0,2.0]],
   "name":"web_devweb01_load",
   "columns":["min1", "min5", "min15"]
  }
]
db.write_points(data)
```

``` ruby
# opentsdb
# put <metric> <timestamp> <value> <tagk1=tagv1[ tagk2=tagv2 ...tagkN=tagvN]>
put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0
```
``` python
# graphite
# <metric> <value> <timestamp>
foo.bar.baz 42 7485784
```

### Read

``` python
# influxdb
result = db.query('select min5 from web_devweb01_load;')
```

``` ruby
# opentsdb
start=1356998400&m=avg:sys.cpu.user{host=*}
```

``` ruby
# graphite
target=sum(stats.counters.fusible.*.*.*.fail.rate),10)&from=-1h
```

最终为了指标监控的使用方便，我们兼容了 graphite 的接口。
