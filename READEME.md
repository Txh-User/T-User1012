# SuperAgg

## Environment
conda env create -f env.yml<br>

conda activate superagg

## Data need to prepare in 'data'
"dataset.csv"<br>
example:<br>
sensorID,boardName,ip,lf,timeStamp,sensorValue,type,recStat<br>
Temp_3,SWM09,192.*.*.*,R3-P15B,2023-04-01 00:22:42,68,nc,0<br>

"rules.csv"<br>
example:<br>
index,confidence,rules<br>
1,0.975,"('c1', 'b1', 'AL1') => ('c2', 'b2', 'AL2')"<br>

"groundtruth.csv"<br>
example:<br>
sensorID,boardName,ip,lf,timeStamp,sensorValue,type,recStat<br>
Volt_1,CPM01,192.*.*.*,R3-P05B,2023-04-01 00:23:02,11.8,dnc,1<br>

## Demo
We provide a demo. Please run:
```
python main.py
