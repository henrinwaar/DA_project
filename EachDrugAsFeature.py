# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 13:37:50 2017

@author: Max
"""

import pandas as pa;
import numpy as np;
from functools import reduce

## Prepare the data
df = pa.read_csv('C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/data/merged_datasetRenamed_withoutBlanks.csv');

## Get the list of different drugs
list_drug1 = df.drug1.unique();
list_drug2 = df.drug2.unique();
list_drug3 = df.drug3.unique();
list_drug4 = df.drug4.unique();
list_drug5 = df.drug5.unique();
list_drug6 = df.drug6.unique();
list_drug7 = df.drug7.unique();
list_drug8 = df.drug8.unique();
list_drug9 = df.drug9.unique();

list_drugs = sorted(list(set(reduce(np.union1d, (list_drug1, list_drug2, list_drug3, list_drug4, list_drug5, list_drug6, list_drug7, list_drug8, list_drug9)))));

## Normalize the continuous features
df2 = df.copy();

meanheight = df2.height.mean();
stdheight = df2.height.std();
meanweight = df2.weight.mean();
stdweight = df2.weight.std();
meanurea = df2.urea.mean();
stdurea = df2.urea.std();
meanmonocytes = df2.monocytes.mean();
stdmonocytes = df2.monocytes.std();
meangranulocytes = df2.granulocytes.mean();
stdgranulocytes = df2.granulocytes.std();
meaneosinophils = df2.eosinophils.mean();
stdeosinophils = df2.eosinophils.std();
meanbasophils = df2.basophils.mean();
stdbasophils = df2.basophils.std();
meanglucose = df2.glucose.mean();
stdglucose = df2.glucose.std();
meanplatelets = df2.platelets.mean();
stdplatelets = df2.platelets.std();
meanmean_platelet_volume = df2.mean_platelet_volume.mean();
stdmean_platelet_volume = df2.mean_platelet_volume.std();
meanleukocytes = df2.leukocytes.mean();
stdleukocytes = df2.leukocytes.std();
meantrgld = df2.trgld.mean();
stdtrgld = df2.trgld.std();
meantflr = df2.tflr.mean();
stdtflr = df2.tflr.std();

df2.height = (df2.height - meanheight)/stdheight;
df2.weight = (df2.weight - meanweight)/stdweight;
df2.urea = (df2.urea - meanurea)/stdurea;
df2.monocytes = (df2.monocytes - meanmonocytes)/stdmonocytes;
df2.granulocytes = (df2.granulocytes - meangranulocytes)/stdgranulocytes;
df2.eosinophils = (df2.eosinophils - meaneosinophils)/stdeosinophils;
df2.basophils = (df2.basophils - meanbasophils)/stdbasophils;
df2.glucose = (df2.glucose - meanglucose)/stdglucose;
df2.platelets = (df2.platelets - meanplatelets)/stdplatelets;
df2.mean_platelet_volume = (df2.mean_platelet_volume - meanmean_platelet_volume)/stdmean_platelet_volume;
df2.leukocytes = (df2.leukocytes - meanleukocytes)/stdleukocytes;
df2.trgld = (df2.trgld - meantrgld)/stdtrgld;
df2.tflr = (df2.tflr - meantflr)/stdtflr;


## Removing correlated features from physician tests
df2 = df2.drop('kidney_absortion_test', 1)
df2 = df2.drop('kidney_enzyme_test', 1)
df2 = df2.drop('kidney_suffering_test', 1)
df2 = df2.drop('kidney_genetic_test(gene_a3hc)', 1)
df2 = df2.drop('kidney_genetic_test(gene_6a3cp)', 1)


df2.to_csv('C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/data/normalizedDatasetWithoutBlanks.csv', index = False);

## Creation of new variables
df_temp = pa.DataFrame(columns = ["patient_id1", "nbDrugs"]);

for e in range(957):
    if (df2.drug1[e] == 'na'):
        nbDrug = 0;
    elif (df2.drug2[e] == 'na'):
        nbDrug = 1;
    elif (df2.drug3[e] == 'na'):
        nbDrug = 2;
    elif (df2.drug4[e] == 'na'):
        nbDrug = 3;
    elif (df2.drug5[e] == 'na'):
        nbDrug = 4;
    elif (df2.drug6[e] == 'na'):
        nbDrug = 5;
    elif (df2.drug7[e] == 'na'):
        nbDrug = 6;
    elif (df2.drug8[e] == 'na'):
        nbDrug = 7;
    elif (df2.drug9[e] == 'na'):
        nbDrug = 8;
    else:
        nbDrug = 9;
    df_temp = df_temp.append({'patient_id1': e+1, 'nbDrugs': nbDrug}, ignore_index=True);
    
## Addition of new variables to the initial data frame
df3 = df2.join(df_temp, on = 'patient_id').drop('patient_id1',1);

df3.to_csv('C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/data/dataset2.0.csv', index = False);


