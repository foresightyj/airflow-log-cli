    npm i -g airflow-log

use:

```

Usage: airflow-log [options]

Options:
  -V, --version            output the version number
  -d, --dagId <dagId>
  -r, --runId <runId>
  -b, --baseUrl <baseUrl>  baseUrl of airflow (default: "http://192.168.0.229:8080")
  -f --toFile              是否将结果写入到文件中，默认是打印到stdout
  -i --keepInfo            是否需要保留airflow日志里面的INFO行，默认是过滤掉INFO行
  -h, --help               display help for command

```
