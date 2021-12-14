#############################################################
########          POC R Studio on MS Azure           ########
########            DRG Predictive Model             ######## 
########                December 2021                ######## 
#############################################################

#Access Azure Portal - Storage Account and Read files
#Data Wrangling and Predictive Model
#Step 1 - Include Azure profile
source("C:/dev/Rprofile.R")
#Step 2 - Invoke necessary libraries for analyses and modeling.
library(AzureStor)    #Manage storage in Microsoft's 'Azure' cloud
library(AzureRMR)     #Interface to 'Azure Resource Manager'
library(psych)        #A general purpose toolbox for personality, psychometric theory and experimental psychology. Functions are primarily for multivariate analysis. 
library(ggplot2) 	    #A system for 'declaratively' creating graphics, based on "The Grammar of Graphics". 
library(caret) 		    #Misc functions for training and plotting classification and regression models.
library(rpart) 		    #Recursive partitioning for classification, regression and survival trees.  
library(rpart.plot) 	#Plot 'rpart' models. Extends plot.rpart() and text.rpart() in the 'rpart' package.
library(RColorBrewer) #Provides color schemes for maps (and other graphics). 
library(party)		    #A computational toolbox for recursive partitioning.
library(partykit)	    #A toolkit with infrastructure for representing, summarizing, and visualizing tree-structure.
library(pROC) 		    #Display and Analyze ROC Curves.
library(ISLR)		      #Collection of data-sets used in the book 'An Introduction to Statistical Learning with Applications in R.
library(randomForest)	#Classification and regression based on a forest of trees using random inputs.
library(dplyr)		    #A fast, consistent tool for working with data frame like objects, both in memory and out of memory.
library(ggraph)		    #The grammar of graphics as implemented in ggplot2 is a poor fit for graph and network visualizations.
library(igraph)		    #Routines for simple graphs and network analysis.
library(mlbench) 	    #A collection of artificial and real-world machine learning benchmark problems, including, e.g., several data sets from the UCI repository.
library(GMDH2)		    #Binary Classification via GMDH-Type Neural Network Algorithms.
library(apex)		      #Toolkit for the analysis of multiple gene data. Apex implements the new S4 classes 'multidna'.
library(mda)		      #Mixture and flexible discriminant analysis, multivariate adaptive regression splines.
library(WMDB)		      #Distance discriminant analysis method is one of classification methods according to multiindex.
library(klaR)		      #Miscellaneous functions for classification and visualization, e.g. regularized discriminant analysis, sknn() kernel-density naive Bayes...
library(kernlab)	    #Kernel-based machine learning methods for classification, regression, clustering, novelty detection.
library(readxl)    	  #n Import excel files into R. Supports '.xls' via the embedded 'libxls' C library.                                                                                                                                                                 
library(GGally)  	    #The R package 'ggplot2' is a plotting system based on the grammar of graphics.                                                                                                                                                                  
library(mctest)		    #Package computes popular and widely used multicollinearity diagnostic measures.
library(sqldf)		    #SQL for dataframe wrangling.
library(reshape2)     #Pivoting table
library(anytime)      #Caches TZ in local env
library(survey)       #Summary statistics, two-sample tests, rank tests, glm.... 
library(mice)         #Library for multiple imputation
library(MASS)         #Functions and datasets to support Venables and Ripley
library(rjson)        #Load the package required to read JSON files.



#Apply credentials from profile
az <- create_azure_login(tenant=Azure_tenantID)
rg <- az$get_subscription(Azure_SubID)$get_resource_group(Azure_ResourceGrp)

#retrieve storage account
stor <- rg$get_storage_account("olastorageac")
stor$get_blob_endpoint()
stor$get_file_endpoint()

# same as above
blob_endp <- blob_endpoint("https://olastorageac.blob.core.windows.net/",key=Azure_Storage_Key)
file_endp <- file_endpoint("https://olastorageac.file.core.windows.net/",key=Azure_Storage_Key)

# shared access signature: read/write access, container+object access and set expiry date
sas <- AzureStor::get_account_sas(blob_endp, permissions="rwcdl", expiry=as.Date("2030-01-01"))
# create an endpoint object with a SAS, but without an access key
blob_endp <- stor$get_blob_endpoint(sas=sas)

#An existing container
Synthea_csv <- blob_container(blob_endp, "syntheacsv")
Synthea_json <- blob_container(blob_endp, "syntheajson")

# list blobs inside a blob container
list_blobs(Synthea_csv)
list_blobs(Synthea_json)

#Temp download of files needed for data wrangling
storage_download(Synthea_csv, "patients.csv", "~/patients.csv")
storage_download(Synthea_csv, "conditions.csv", "~/conditions.csv")
storage_download(Synthea_csv, "encounters.csv", "~/encounters.csv")
storage_download(Synthea_csv, "observations_adjusted.csv", "~/observations_adjusted.csv")
storage_download(Synthea_csv, "msdrg.csv", "~/msdrg.csv")

#Read csv in memory
patients<-read.csv("patients.csv")
conditions<-read.csv("conditions.csv")
encounters<-read.csv("encounters.csv")
observations<-read.csv("observations_adjusted.csv")
msdrg<-read.csv("msdrg.csv")

#Delete Temp downloaded of files
file.remove("patients.csv")
file.remove("conditions.csv")
file.remove("encounters.csv")
file.remove("observations_adjusted.csv")
file.remove("msdrg.csv")

###########WIP VIEW JSON FILE & MANIPULATE##########

#Data cleaning and wrangling to get features required for modeling

#Number of columns
ncol(patients)
#Number of rows
nrow(patients)
#View fields in files
names(patients)
#View subset of file
head(patients,2)
#View structure of file
str(patients)

#Step 3
#Descriptive Statistics 
summary(patients)

#Select Specific Data Elements from Patients, Encounters and Conditions 
obs_pat_enc_con<-sqldf("select x.*,y.PATIENT_ID,y.START,y.STOP,y.ENCOUNTERCLASS,y.BIRTHDATE,y.DEATHDATE,y.MARITAL,y.RACE,y.ETHNICITY,y.GENDER,y.DESCRIPTION from new_obs x 
left join pat_enc_con y on x.ENCOUNTER_ID=y.ENCOUNTER_ID")
#Data wrangling from Patients, Encounters and Conditions merged dataframes
pat_enc_con<-sqldf("select x.Id as ENCOUNTER_ID, x.PATIENT as PATIENT_ID,x.START,x.STOP,x.ENCOUNTERCLASS,y.BIRTHDATE,y.DEATHDATE,y.MARITAL,y.RACE,y.ETHNICITY,y.GENDER,z.DESCRIPTION from encounters x 
left join patients y on x.PATIENT=y.Id
left join conditions z on x.Id=z.ENCOUNTER")
#Data wrangling from Observations
obs<-sqldf("select distinct ENCOUNTER as ENCOUNTER_ID,DESCRIPTION as ENC_DESCRIPTION,VALUE from observations")
#Pivot table to align with prior datasets - Interested in numerical data for this specific work
new_obs<- dcast(obs,  ENCOUNTER_ID ~ ENC_DESCRIPTION,value.var = "VALUE",fun.aggregate = sum)
#Merge Observations, Patients, Encounters and Conditions Data
obs_pat_enc_con<-sqldf("select x.*,y.PATIENT_ID,y.START,y.STOP,y.ENCOUNTERCLASS,y.BIRTHDATE,y.DEATHDATE,y.MARITAL,y.RACE,y.ETHNICITY,y.GENDER,y.DESCRIPTION from new_obs x 
left join pat_enc_con y on x.ENCOUNTER_ID=y.ENCOUNTER_ID")
#Merge obs_pat_enc_con with MS-DRG data
msdrg_Len<-sqldf("select a.* ,length(msdrg) as length from msdrg a")
msdrg_Len$MSDRG <- as.character(msdrg_Len$MSDRG)
msdrg_new<-sqldf("select a.* , 
case when length<3 then 0||msdrg
     else msdrg
end as CMSDRG from msdrg_Len a")
obs_pat_enc_con_drg<-sqldf("select x.*, y.MSDRGDESCRIPTION,y.CMSDRG from obs_pat_enc_con x left join msdrg_new y on x.DESCRIPTION=y.CONDITIONDESCRIPTION")
#New dataframe
newdataframe<-obs_pat_enc_con_drg
#Create calculated fields
newdataframe$Discharge_Date<-anytime(newdataframe$STOP)
newdataframe$Admit_Date<-anytime(newdataframe$START)
newdataframe$LOS_Days <- as.numeric((newdataframe$Discharge_Date-newdataframe$Admit_Date)/1440)
newdataframe$DOB<-anytime(newdataframe$BIRTHDATE)
newdataframe$Age_Years_at_Encounter <- as.numeric((newdataframe$Admit_Date-newdataframe$DOB)/8760)
#Relevant fields with sizable data
Relevant_df<-sqldf("select distinct PATIENT_ID, ENCOUNTERCLASS,
MARITAL,RACE,ETHNICITY,GENDER,Age_Years_at_Encounter,LOS_Days,
Painseverity0to10verbalnumericratingScore_Reported_score,
BodyHeight_cm,
BodyWeight_kg,
DiastolicBloodPressure_mmHg,
Heartrate_permin,
Respiratoryrate_permin,
SystolicBloodPressure_mmHg,
Bodytemperature_Cel,
BodyMassIndex_kgperm2,MSDRGDESCRIPTION as CMSDRGDESCRIPTION,CMSDRG from newdataframe where CMSDRG is not null")
#Obtain distributions of variables
features_plots <- lapply(names(Relevant_df), function(var_x){
  p <-  ggplot(Relevant_df) + aes_string(var_x)
  if(is.numeric(Relevant_df[[var_x]])) {
    p <- p + geom_density() } else {
      p <- p + geom_bar()}})
features_plots
#Recoding variables
#ENCOUNTERCLASS
Relevant_df$ENCOUNTERCLASSF[Relevant_df$ENCOUNTERCLASS == "ambulatory"] <- "1"
Relevant_df$ENCOUNTERCLASSF[Relevant_df$ENCOUNTERCLASS == "emergency"] <- "2"
Relevant_df$ENCOUNTERCLASSF[Relevant_df$ENCOUNTERCLASS == "inpatient"] <- "3"
Relevant_df$ENCOUNTERCLASSF[Relevant_df$ENCOUNTERCLASS == "outpatient"] <- "4"
Relevant_df$ENCOUNTERCLASSF[Relevant_df$ENCOUNTERCLASS == "urgentcare"] <- "5"
Relevant_df$ENCOUNTERCLASSF[Relevant_df$ENCOUNTERCLASS == "wellness"] <- "6"
Relevant_df$ENCOUNTERCLASSF[Relevant_df$ENCOUNTERCLASS == ""] <- NA
Relevant_df$ENCOUNTERCLASSF <- as.factor(Relevant_df$ENCOUNTERCLASSF)
#MARITAL
Relevant_df$MARITALF[Relevant_df$MARITAL == "M"] <- "1"
Relevant_df$MARITALF[Relevant_df$MARITAL == "S"] <- "2"
Relevant_df$MARITALF[Relevant_df$MARITAL == ""] <- NA
Relevant_df$MARITALF <- as.factor(Relevant_df$MARITALF)
#RACE
Relevant_df$RACEF[Relevant_df$RACE == "asian"] <- "1"
Relevant_df$RACEF[Relevant_df$RACE == "black"] <- "2"
Relevant_df$RACEF[Relevant_df$RACE == "native"] <- "3"
Relevant_df$RACEF[Relevant_df$RACE == "other"] <- "4"
Relevant_df$RACEF[Relevant_df$RACE == "white"] <- "5"
Relevant_df$RACEF[Relevant_df$RACE == ""] <- NA
Relevant_df$RACEF <- as.factor(Relevant_df$RACEF)
#GENDER 
Relevant_df$GENDERF[Relevant_df$GENDER == "M"] <- "1"
Relevant_df$GENDERF[Relevant_df$GENDER == "F"] <- "2"
Relevant_df$GENDERF[Relevant_df$GENDER == ""] <- NA
Relevant_df$GENDERF <- as.factor(Relevant_df$GENDERF)
#ETHNICITY
Relevant_df$ETHNICITYF[Relevant_df$ETHNICITY == "hispanic"] <- "1"
Relevant_df$ETHNICITYF[Relevant_df$ETHNICITY == "nonhispanic"] <- "2"
Relevant_df$ETHNICITYF[Relevant_df$ETHNICITY == ""] <- NA
Relevant_df$ETHNICITYF <- as.factor(Relevant_df$ETHNICITYF)
#Select relevant fields
Relevant_df_1<-sqldf("select distinct PATIENT_ID, ENCOUNTERCLASSF as ENCOUNTERCLASS,
MARITALF as MARITAL,RACEF as RACE,ETHNICITYF as ETHNICITY,GENDERF as GENDER,Age_Years_at_Encounter,LOS_Days,
Painseverity0to10verbalnumericratingScore_Reported_score,
BodyHeight_cm,
BodyWeight_kg,
DiastolicBloodPressure_mmHg,
Heartrate_permin,
Respiratoryrate_permin,
SystolicBloodPressure_mmHg,
Bodytemperature_Cel,
BodyMassIndex_kgperm2,CMSDRGDESCRIPTION,CMSDRG from Relevant_df")

#Preliminaries - Tests and Analysis
#Normality Testing
#Age_Years_at_Encounter
#LOS_Days
#Painseverity0to10verbalnumericratingScore_Reported_score
#BodyHeight_cm
#BodyWeight_kg
#DiastolicBloodPressure_mmHg
#Heartrate_permin
#Respiratoryrate_permin
#SystolicBloodPressure_mmHg
#Bodytemperature_Cel
#BodyMassIndex_kgperm2
#Ideally, Multivariate Normality Testing would have been suitable - Research in This area -WIP.
qqnorm(Relevant_df_1$BodyHeight_cm)
qqline(Relevant_df_1$BodyHeight_cm)
#The null hypothesis for this test is that the data are normally distributed. The Prob < W value listed in #the #output is the p-value. If the chosen alpha level is 0.05 and the p-value is less than 0.05, then the #null #hypothesis that the data are normally distributed is rejected. If the p-value is greater than 0.05, #then the #null hypothesis has not been rejected.
shapiro.test(Relevant_df_1$BodyHeight_cm)
#Multicollinearity conducted via Variance 
#Inflation Factor (VIF), validated for significance using Akaike #Information Criteria (AIC), Bayesian #Information Criteria (BIC) and excluded some explanatory #variables.
X<-subset (Relevant_df_1, select=c(
  Age_Years_at_Encounter,LOS_Days,Painseverity0to10verbalnumericratingScore_Reported_score,BodyHeight_cm,
  BodyWeight_kg,DiastolicBloodPressure_mmHg,Heartrate_permin,Respiratoryrate_permin,SystolicBloodPressure_mmHg,
  Bodytemperature_Cel))
ggpairs(X)
#Replacing missing with median
Relevant_df_1$ENCOUNTERCLASS[is.na(Relevant_df_1$ENCOUNTERCLASS)]<-median(Relevant_df_1$ENCOUNTERCLASS,na.rm=TRUE)
Relevant_df_1$MARITAL[is.na(Relevant_df_1$MARITAL)]<-median(Relevant_df_1$MARITAL,na.rm=TRUE)
Relevant_df_1$RACE[is.na(Relevant_df_1$RACE)]<-median(Relevant_df_1$RACE,na.rm=TRUE)
Relevant_df_1$ETHNICITY[is.na(Relevant_df_1$ETHNICITY)]<-median(Relevant_df_1$ETHNICITY,na.rm=TRUE)
Relevant_df_1$GENDER[is.na(Relevant_df_1$GENDER)]<-median(Relevant_df_1$GENDER,na.rm=TRUE)
#Non-zero medians [conditional -median(x[x>0])...mean(x[x>0])
Relevant_df_1$Painseverity_M<-median(Relevant_df_1$Painseverity0to10verbalnumericratingScore_Reported_score[Relevant_df_1$Painseverity0to10verbalnumericratingScore_Reported_score>0])
Relevant_df_1$BodyHeight_M<-median(Relevant_df_1$BodyHeight_cm[Relevant_df_1$BodyHeight_cm>0])
Relevant_df_1$BodyWeight_M<-median(Relevant_df_1$BodyWeight_kg[Relevant_df_1$BodyWeight_kg>0])
Relevant_df_1$DiastolicBloodPressure_M<-median(Relevant_df_1$DiastolicBloodPressure_mmHg[Relevant_df_1$DiastolicBloodPressure_mmHg>0])
Relevant_df_1$Heartrate_M<-median(Relevant_df_1$Heartrate_permin[Relevant_df_1$Heartrate_permin>0])
Relevant_df_1$Respiratoryrate_M<-median(Relevant_df_1$Respiratoryrate_permin[Relevant_df_1$Respiratoryrate_permin>0])
Relevant_df_1$SystolicBloodPressure_M<-median(Relevant_df_1$SystolicBloodPressure_mmHg[Relevant_df_1$SystolicBloodPressure_mmHg>0])
Relevant_df_1$Bodytemperature_M<-median(Relevant_df_1$Bodytemperature_Cel[Relevant_df_1$Bodytemperature_Cel>0])
Relevant_df_1$BodyMassIndex_M<-median(Relevant_df_1$BodyMassIndex_kgperm2[Relevant_df_1$BodyMassIndex_kgperm2>0])
#Data ready for modeling
Relevant_df_2<-sqldf("select distinct PATIENT_ID,CMSDRGDESCRIPTION,ENCOUNTERCLASS,MARITAL,RACE,ETHNICITY,GENDER,Age_Years_at_Encounter,LOS_Days,
case when Painseverity0to10verbalnumericratingScore_Reported_score=0 then Painseverity_M else Painseverity0to10verbalnumericratingScore_Reported_score end as Painseverity0to10, 
case when BodyHeight_cm=0 then BodyHeight_M else BodyHeight_cm end as BodyHeight_incm,
case when BodyWeight_kg=0 then BodyWeight_M else BodyWeight_kg end as BodyWeight_inkg,
case when DiastolicBloodPressure_mmHg=0 then DiastolicBloodPressure_M else DiastolicBloodPressure_mmHg end as DiastolicBloodPressure_inmmHg,
case when Heartrate_permin=0 then Heartrate_M else Heartrate_permin end as Heartrate_ipermin,
case when Respiratoryrate_permin=0 then Respiratoryrate_M else Respiratoryrate_permin end as Respiratoryrate_ipermin,
case when SystolicBloodPressure_mmHg=0 then SystolicBloodPressure_M else SystolicBloodPressure_mmHg end as SystolicBloodPressure_inmmHg,
case when Bodytemperature_Cel=0 then Bodytemperature_M else Bodytemperature_Cel end as Bodytemperature_inCel,
case when BodyMassIndex_kgperm2=0 then BodyMassIndex_M else BodyMassIndex_kgperm2 end as BodyMassIndex_inkgperm2,
CMSDRG from Relevant_df_1")


#Modeling
#Setting the random seed for replication
set.seed(1234)
Model_df<-Relevant_df_2[,c(3:19)]

#setting up cross-validation
cv_control <- trainControl(method="repeatedcv", number = 10, allowParallel=TRUE)

#Variable of Importance
vip_test <- train(as.factor(CMSDRG) ~ ., data=Model_df, method="treebag", trControl=cv_control, importance=TRUE)
plot(varImp(vip_test))
#Features to be used for ML
split_df<-Model_df[,c(6:17)]

#80/20 split
#create a list of random number 80%/20% split  
dataframe = sort(sample(nrow(split_df), nrow(split_df)*.8))
#creating training data 
train<-data[dataframe ,]
#creating test data set 
test<-data[-dataframe,]

#Random Forest for Classification Trees using method="rf"
#Assumptions
#a.No formal distributional assumptions, random forests are non-parametric and can thus handle skewed and multi-modal data.
#NOTE:- This particular algorithm may take a long time to run, may need to recalibrate using smaller samples
ranfor <- train(as.factor(CMSDRG) ~ ., data=split_df,method="rf",trControl=cv_control, importance=TRUE)
ranfor 
#Get class predictions for training dataset
class_ranfor <-  predict(ranfor, type="raw")
head(ranfor)
#Get class predictions for test dataset
testranfor <-  predict(ranfor, newdata = split_df, type="raw")
head(testranfor)
#Derive predicted probabilites for test dataset
probsranfor=predict(ranfor, newdata=split_df, type="prob")
head(probsranfor)
#confusionmatrix
confusionmatrixranfor<-confusion(predict(ranfor, split_df), split_df$CMSDRG)
confusionmatrixranfor
#Overall Misclassification Rate
errorrateranfor<-(1-sum(diag(confusionmatrixranfor))/sum(confusionmatrixranfor))
errorrateranfor
#Sensitivity and Specificity
# Sensitivity - aka true positive rate, the recall, or probability of detection
sensitivityranfor<-sensitivity(confusionmatrixranfor)
sensitivityranfor
## Specificity - aka true negative rate
specificityranfor<-specificity(confusionmatrixranfor)
specificityranfor
#prediction
predranfor<-data.frame(testranfor)
final<- cbind(Relevant_df, predranfor)
Final_Result<-sqldf("select distinct
x.PATIENT_ID,          
x.ENCOUNTERCLASS,
x.MARITAL,
x.RACE,
x.ETHNICITY,
x.GENDER,
x.Age_Years_at_Encounter,
x.LOS_Days,
x.Painseverity0to10verbalnumericratingScore_Reported_score,
x.BodyHeight_cm,
x.BodyWeight_kg,
x.DiastolicBloodPressure_mmHg,
x.Heartrate_permin,
x.Respiratoryrate_permin,
x.SystolicBloodPressure_mmHg,
x.Bodytemperature_Cel,
x.BodyMassIndex_kgperm2,      
x.CMSDRG as TRUE_CMSDRG,
x.testranfor as PREDICTED_CMSDRG,
x.CMSDRGDESCRIPTION as TRUE_CMSDRGDESCRIPTION
from final x")
#Write output of prediction to csv 
write.csv(Final_Result, file = "Final_Result_csv.csv")
#Creating the data for JSON file
jsonData <- toJSON(Final_Result)
write(jsonData,"Final_Result_json.json")
#Upload Model results data into container in Azure
cont_upload <- blob_container(blob_endp, "modeloutput")
upload_blob(cont_upload, src="C:\\Users\\olajideajayi\\OneDrive - Microsoft\\Documents\\Final_Result_csv.csv")
upload_blob(cont_upload, src="C:\\Users\\olajideajayi\\OneDrive - Microsoft\\Documents\\Final_Result_json.json")