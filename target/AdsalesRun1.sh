#First code to create tables
#spark-submit --class com.viacom18.Adsales.Adsales_bootstrap_Load --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14 /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"


#Second code to populate tables
#spark-submit --class com.viacom18.Adsales.AdsalesViews1 --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14 /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

#spark-submit --class com.viacom18.Adsales.AdsalesViews2 --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14 /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

spark-submit --class com.viacom18.Adsales.Ads_Content_Viewer_show --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14 /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

spark-submit --class com.viacom18.Adsales.Ads_Content_Viewer_all --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14 /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

spark-submit --class com.viacom18.Adsales.Ads_Content_Viewer_state --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14  /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

spark-submit --class com.viacom18.Adsales.Ads_Content_Viewer_city --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14  /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

spark-submit --class com.viacom18.Adsales.Ads_Viewer_all --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14  /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

spark-submit --class com.viacom18.Adsales.Ads_Viewer_city --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14  /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

spark-submit --class com.viacom18.Adsales.Ads_Viewer_age --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14  /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

spark-submit --class com.viacom18.Adsales.Ads_Viewer_state --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14  /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"

#Third code to external to internal
#spark-submit --class com.viacom18.Adsales.Adsales_External_Load --master yarn --deploy-mode cluster --executor-memory 22g --executor-cores 14 --num-executors 14  /home/v18biprodssh/v18/voot/migration/newjars/Project_Pi-1.0-SNAPSHOT.jar "Weekly" "2018-05-04"
