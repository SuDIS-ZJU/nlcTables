## Dataset 
Our nlcTables supports NL-only table search (nlcTables_K), NL-conditional table union search (nlcTables-U), and NL-conditional table join search (nlcTables-J). For union and join tasks, fuzzy versions (nlcTables-U-fz and nlcTables-J-fz) are provided using
semantic augmentation. You can download these five datasets in Table 1. In total, nlcTables contains 22,080 tables
with large average size and includes 21,200 labeled GTs. The more detailed statistics are shown in Table 2.

|                 Datasets                    | Download |
| :-----------------------------------------: | :-----------------------------------------: | 
|NL-only table search (nlcTables_K)|        [Download](https://drive.google.com/drive/folders/1EsIwoBAHJXmlNoJWO50BSevJDbaj-653?usp=drive_link)     |
|NL-conditional table union search (nlcTables-U)|       [Download](https://drive.google.com/drive/folders/1uvAEzvNl6F_mW_ygv2ciJzY39WWrbg1A?usp=drive_link)       |    
|nlcTables-U-fz|       [Download](https://drive.google.com/drive/folders/1-3cUUYK0NjcfmbzNB6AifmCNJTW1oM7C?usp=drive_link)       |    
|NL-conditional table join search (nlcTables-J)|       [Download](https://drive.google.com/drive/folders/1lfmfYzDii2C4StZjKJhSdPg94dypuvJJ?usp=drive_link)       |  
|nlcTables-U-fz|       [Download](https://drive.google.com/drive/folders/10MLJg4Vu08i8NKFfyLBVbQpc3RX806PR?usp=drive_link)       |     

<div align="center">
    <img src="images/stats.jpeg" width="1000px">
    <p style="font-size: 20px; font-weight: bold; margin-top: 10px;"> </p>
</div>

<span id="-getstart"></span>
## GettingStart
This is an example of how to construct your own nlcTD datasets.

```sh
python union.py
```
