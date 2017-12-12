# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 12:40:55 2017

@author: Max
"""
## Merging drug.csv and kidney_fail_dataset.csv

import pandas as pd

a = pd.read_csv("C:/Users/Max/Documents/UPM/Data_Analysis/Projet/drugs.csv")
b = pd.read_csv("C:/Users/Max/Documents/UPM/Data_Analysis/Projet/kidney_fail_dataset.csv")
b = b.dropna(axis=1)
merged = a.merge(b, on='patient_id')
merged.to_csv("C:/Users/Max/Documents/UPM/Data_Analysis/Projet/merged_dataset.csv", index=False)