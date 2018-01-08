# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 13:37:50 2017

@author: Henri DESQUESSES & Thomas JASSEM
"""

import pandas as pa;
import numpy as np;
from functools import reduce;
import difflib as diff;

def similar(a,b):
    return diff.SequenceMatcher(None,a,b).ratio();


## Merging drug.csv and kidney_fail_dataset.csv
a = pa.read_csv("C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/Data/drugs.csv")
b = pa.read_csv("C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/Data/kidney_fail_dataset.csv")
b = b.dropna(axis=1)
merged = a.merge(b, on='patient_id')
merged.to_csv("C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/Data/merged_dataset.csv", index=False)

## After we use an other data set, result of manual modifications with open refine and Spark done 
## on the data set we just built 

## Prepare the data
df = pa.read_csv('C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/data/sparkFilled.csv');

## Get the list of different drugs
list_drug1 = df.drug1.unique();
c1 = 0;
matches = [];
for i in range(len(list_drug1)):
    match = diff.get_close_matches(list_drug1[i], matches, 1, 0.4);
    if match:
        list_drug1[i] = match[0];
        c1 += 1;
    else:
        matches.append(list_drug1[i]);

list_drug2 = df.drug2.unique();
c2 = 0;
for i in range(len(list_drug2)):
    match = diff.get_close_matches(list_drug2[i], list_drug1, 1, 0.4);
    if match:
        list_drug2[i] = match[0];
        c2 += 1;
    
list_drug3 = df.drug3.unique();
c3 = 0;
for i in range(len(list_drug3)):
    match = diff.get_close_matches(list_drug3[i], list_drug1, 1, 0.4);
    if match:
        list_drug3[i] = match[0];
        c3 += 1;
        
list_drug4 = df.drug4.unique();
c4 = 0;
for i in range(len(list_drug4)):
    match = diff.get_close_matches(list_drug4[i], list_drug1, 1, 0.4);
    if match:
        list_drug4[i] = match[0];
        c4 += 1;
        
list_drug5 = df.drug5.unique();
c5 = 0;
for i in range(len(list_drug5)):
    match = diff.get_close_matches(list_drug5[i], list_drug1, 1, 0.4);
    if match:
        list_drug5[i] = match[0];
        c5 += 1;
        
list_drug6 = df.drug6.unique();
c6 = 0;
for i in range(len(list_drug6)):
    match = diff.get_close_matches(list_drug6[i], list_drug1, 1, 0.4);
    if match:
        list_drug6[i] = match[0];
        c6 += 1;
        
list_drug7 = df.drug7.unique();
c7 = 0;
for i in range(len(list_drug7)):
    match = diff.get_close_matches(list_drug7[i], list_drug1, 1, 0.4);
    if match:
        list_drug7[i] = match[0];
        c7 += 1;
        
list_drug8 = df.drug8.unique();
c8 = 0;
for i in range(len(list_drug8)):
    match = diff.get_close_matches(list_drug8[i], list_drug1, 1, 0.4);
    if match:
        list_drug8[i] = match[0];
        c8 += 1;
        
list_drug9 = df.drug9.unique();
c9 = 0;
for i in range(len(list_drug9)):
    match = diff.get_close_matches(list_drug9[i], list_drug1, 1, 0.4);
    if match:
        list_drug9[i] = match[0];
        c9 += 1;

list_drugs = sorted(list(set(reduce(np.union1d, (list_drug1, list_drug2, list_drug3, list_drug4, list_drug5, list_drug6, list_drug7, list_drug8, list_drug9)))));
matches = [];
for i in range(len(list_drugs)):
    match = diff.get_close_matches(list_drugs[i], matches, 1, 0.4);
    if match:
        list_drugs[i] = match[0];
        c1 += 1;
    else:
        matches.append(list_drugs[i]);
pa.DataFrame({"Drugs":list_drugs}).drop_duplicates().to_csv('C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/data/listDrugs.csv', index = False);

## Replace with the matched drug in the data set
c11 = 0;
for i in range(len(df.drug1)):
    match = diff.get_close_matches(df.drug1[i], list_drugs, 1, 0.4);
    if match != []:
        df.set_value(i, 'drug1', match[0]);
        c11 += 1;

c22 = 0;
for i in range(len(df.drug2)):
    match = diff.get_close_matches(df.drug2[i], list_drugs, 1, 0.4);
    if match != []:
        df.set_value(i, 'drug2', match[0]);
        c22 += 1;
        
c33 = 0;
for i in range(len(df.drug3)):
    match = diff.get_close_matches(df.drug3[i], list_drugs, 1, 0.4);
    if match != []:
       df.set_value(i, 'drug3', match[0]);
       c33 += 1;
        
c44 = 0;
for i in range(len(df.drug4)):
    match = diff.get_close_matches(df.drug4[i], list_drugs, 1, 0.4);
    if match != []:
       df.set_value(i, 'drug4', match[0]);
       c44 += 1;
        
c55 = 0;
for i in range(len(df.drug5)):
    match = diff.get_close_matches(df.drug5[i], list_drugs, 1, 0.4);
    if match != []:
       df.set_value(i, 'drug5', match[0]);
       c55 += 1;
        
c66 = 0;
for i in range(len(df.drug6)):
    match = diff.get_close_matches(df.drug6[i], list_drugs, 1, 0.4);
    if match != []:
       df.set_value(i, 'drug6', match[0]);
       c66 += 1;
        
c77 = 0;
for i in range(len(df.drug7)):
    match = diff.get_close_matches(df.drug7[i], list_drugs, 1, 0.4);
    if match != []:
       df.set_value(i, 'drug7', match[0]);
       c77 += 1;
        
c88 = 0;
for i in range(len(df.drug8)):
    match = diff.get_close_matches(df.drug8[i], list_drugs, 1, 0.4);
    if match != []:
        df.set_value(i, 'drug8', match[0]);
        c88 += 1;
        
c99 = 0;
for i in range(len(df.drug9)):
    match = diff.get_close_matches(df.drug9[i], list_drugs, 1, 0.4);
    if match != []:
       df.set_value(i, 'drug9', match[0]);
       c99 += 1;

## Removing correlated features from physician tests
df2 = df.copy();
df2 = df2.drop('kidney_absortion_test', 1);
df2 = df2.drop('kidney_enzyme_test', 1);
df2 = df2.drop('kidney_suffering_test', 1);
df2 = df2.drop('kidney_genetic_test(gene_a3hc)', 1);
df2 = df2.drop('kidney_genetic_test(gene_6a3cp)', 1);

## Creation of new variables


## Number of drugs
df_temp = pa.DataFrame(columns = ["nbDrugs"]);
for e in range(957):
    nbDrug = 0;
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
    df_temp = df_temp.append({'nbDrugs': nbDrug}, ignore_index = True);
    
## Drugs as a unique string feature
df_temp2 = pa.DataFrame(columns = ["Drugs"]);
for e in range(957):
    listDrugs = [];
    listDrugs.append(df2.drug1[e]);
    listDrugs.append(df2.drug2[e]);
    listDrugs.append(df2.drug3[e]);
    listDrugs.append(df2.drug4[e]);
    listDrugs.append(df2.drug5[e]);
    listDrugs.append(df2.drug6[e]);
    listDrugs.append(df2.drug7[e]);
    listDrugs.append(df2.drug8[e]);
    listDrugs.append(df2.drug9[e]);
    listDrugs = sorted(listDrugs);
    drugs = '#'.join(listDrugs);
    df_temp2 = df_temp2.append({'Drugs': drugs}, ignore_index = True);

## Calcul de l IMC
## (categories described in annex)
df_temp3 = pa.DataFrame(columns = ["IMC"]);
for e in range(957):
    weight = df2.weight[e];
    height2 = pow(df2.height[e], 2);
    imc = (weight / height2) * 703;
    cat = 2;
    if imc < 16.5:
        cat = 0;
    elif 16.5 <= imc < 18.5:
        cat = 1;
    elif 18.5 <= imc < 25:
        cat = 2;
    elif 25 <= imc < 30:
        cat = 3;
    elif 30 <= imc < 35:
        cat = 4;
    elif 35 <= imc < 40:
        cat = 5;
    elif 40 <= imc:
        cat = 6;
    df_temp3 = df_temp3.append({'IMC': cat}, ignore_index = True);
    
## Addition of new variables to the initial data frame
##NbDrugs
df2 = df2.join(df_temp);
##Drugs
df2 = df2.join(df_temp2);
##IMC
df2 = df2.join(df_temp3);

## Normalize the continuous features
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

##Write the final data set in a csv file
df2.to_csv('C:/Users/Max/Documents/UPM/Data_Analysis/Projet/src/data/dataset2.0.csv', index = False);