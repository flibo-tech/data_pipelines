# data_pipelines
This repository contains all the data pipelines for FLIBO

### NOTE 
While using SSH for EC2 spot instances, after installing all the requirements from requirements.txt go to following paramiko_expect library file 
#####...\venv\Lib\site-packages\paramiko_expect.py
and change line number 151

from

```python
current_buffer_decoded = current_buffer.decode(self.encoding)
```

to

```pytohn
current_buffer_decoded = current_buffer.decode(self.encoding, errors="ignore")
```
