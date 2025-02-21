## :hammer: GettingStart
This is an example of how to construct your own nlcTD datasets. Remember to change the file paths.

### Quick Start

1. Generate NL-conditional unionable table search dataset.
```sh
python union.py
```

2. Generate NL-conditional joinable table search dataset.
```sh
python join.py
```

### How to generate your own nlcTD dataset?

1. Use your own original table for splitting. You have to change your table into json file.
```sh
{
    "title": [
        "Hancock St & Cottage Ave",
        "Quincy Ave Opp President Plaza",
        "Washington St & Broad St",
        "Commercial St Opp Brookside Rd"
    ],
    "numCols": 4,
    "numericColumns": [],
    "dateColumns": [
        0,
        1,
        2,
        3
    ],
    "pgTitle": "",
    "numDataRows": 26,
    "secondTitle": "",
    "numHeaderRows": 1,
    "caption": "Bus schedule",
    "data": [
        [
            "09:00 AM",
            " ",
            "08:44 AM",
            "08:46 AM"
        ],
        [
            "08:55 AM",
            "08:51 AM",
            "08:40 AM",
            "08:42 AM"
        ],
        [
            "11:29 AM",
            " ",
            "11:12 AM",
            "11:13 AM"
        ]
    ]
}
```


2. Change hyper-parameter. We have constructed several splitting fuction based on our taxonomy, for example the theme query at table level has the split_theme_table function. You can change these parameter when you call these functions.

```sh
def split_theme_table(index, json_file, query_folder, datalake_folder, query_txt, groundtruth_txt, ori_minRow=10, max_duplicate=0.1, min_split_rate=0.2, template_num=3, shuffle=1, neg_num = 10,pos_num = 5):
