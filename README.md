# SuperAgg
SuperAgg is an alert aggregation framework dedicated to the out-of-band monitoring of supercomputer systems. It uses automatically pattern learning to detect potential patterns in alert bursts and pattern-aware aggregation strategies for redundant alert suppression. The learning and aggregation workflow should be generic for alert management, and even time series refinement. Please feel free for trying.

## Environment
```
conda env create -f env.yml
conda activate superagg
```

## Data description
"dataset.csv"<br>
"rules.csv"<br>
"groundtruth.csv"<br>
Note: The data has been desensitized. One should modify the data pharsing logic in the code to adapt to their own contexts.

## Demo
To run the toy demo, please use:
```
python main.py
```

## BibTex
```
@inproceedings{yuan2024superagg,
  author={Yuan, Yuan and Zhou, Tongqing and Tan, Xiuhong and Sun, Yongqian and Li, Yuqi and Li, Zhixing and Cai, Zhiping and Li, Tiejun},
  booktitle={IEEE 35th International Symposium on Software Reliability Engineering (ISSRE)}, 
  title={Exploring Hierarchical Patterns for Alert Aggregation in Supercomputers}, 
  year={2024}
}
```
