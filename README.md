# data_pipelines
This repository contains all the data pipelines for FLIBO

### NOTE 
While using SSH for EC2 spot instances, after installing all the requirements from requirements.txt go to following paramiko_expect library file

...\venv\Lib\site-packages\paramiko_expect.py

and change line number 151

from

```python

# line 37
sys.stdout.flush()

# line 151
current_buffer_decoded = current_buffer.decode(self.encoding)
```

to

```python
# line 37
try:
    sys.stdout.flush()
except:
    print('System could not decode expected output.')

# line 151
current_buffer_decoded = current_buffer.decode(self.encoding, errors="ignore")
```
