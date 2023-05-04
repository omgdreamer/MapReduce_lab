## 1. 

>  用 Python 完成 MapReduce 实例统计输入文件的单词的词频

- 输入：文本文件
- 输出：单词和词频信息，用 `\t` 隔开



## 2. MapReduce两个阶段

- **Map**：产生词与次数标记键值对
- **Reduce**：聚合同一个词(key)的值，完成统计



### 2.1 Map阶段

```python
import sys
for line in sys.stdin:
    line = line.strip()
    words = line.split()
    for word in words:
        print "%s\t%s" % (word, 1)
```

- Map 脚本不会计算单词的总数，而是直接输出 `<word> 1`，主要是把单词切开。



### 2.2 Reduce阶段



``` python
from operator import itemgetter
import sys
current_word = None
current_count = 0
word = None
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:  #count如果不是数字的话，直接忽略掉
        continue
    if current_word == word:
        current_count += count
    else:
        if current_word:
            print "%s\t%s" % (current_word, current_count)
        current_count = count
        current_word = word
if word == current_word:  #不要忘记最后的输出
    print "%s\t%s" % (current_word, current_count)

```



