rm(list= ls(all=TRUE))

library(data.table)
library(fastDummies)
library(randomForest)
library(DBI)
library(odbc)


con <- DBI::dbConnect(odbc::odbc(), driver = "SQL Server", dsn = "ZipAnalyticsADW", 
                      uid = "zipcode_analytics_app", pwd = "DECZr91@cF")


setwd('D:/harsh.vardhana/jan_fe/')

sp_b=fread('HE_imports_feature_extract_time_series_2019_01_11.csv')

rtlr_map=dbGetQuery(con,"select rtlr_party_id, channel, rtlr_num from zip_analytics_test.rtlr_geo_lookup where rtlr_party_id=rtlr_num")

sp_b$rtlr_party_id=as.numeric(sp_b$rtlr_party_id)
rtlr_map$rtlr_party_id=as.numeric(rtlr_map$rtlr_party_id)

sp_b=merge(sp_b,rtlr_map,by='rtlr_party_id')
sp_b=sp_b[channel=='LIQUOR']

vol_dat=fread('fin_vol_dat_pkgliqSF.csv')
offsets=fread('offsets_pkgliqSF.csv')
mpd=fread('mpd_month_offpremise.csv')

sp_b$lola_grp=tolower(as.character(sp_b$brnd_cd))

var_map=data.frame(org=names(sp_b)[6:309],new=paste0('v_',1:304))

names(sp_b)[6:309]=paste0('v_',1:304)

names(sp_b)[grep('v_',names(sp_b))]

sp_b$rtlr_num=as.numeric(sp_b$rtlr_num)
#class(sp_b$cal_yr_mo_nbr)
fin_data=merge(vol_dat[,.(rtlr_num,vol_sales,ptr,cal_yr_mo_nbr,lola_grp,dma_key,year,month,xcyea06v001,cyec17v001,xcyfem,store_fixed,store_pres,state_cd)],
              sp_b[,c('rtlr_num','lola_grp','cal_yr_mo_nbr',paste0('v_',1:304))],by= c('rtlr_num','lola_grp','cal_yr_mo_nbr'))

fin_data=merge(fin_data,unique(offsets[,c('dma_key','year','month','lola_grp','recency_offset','recency_offset2')]),by=c('dma_key','year','month','lola_grp'),all.x = T)
fin_data=merge(fin_data,unique(offsets[!is.na(offsets$regional_offset),c('dma_key','year','lola_grp','regional_offset')]),by=c('dma_key','year','lola_grp'),all.x = T)
fin_data=merge(fin_data,unique(offsets[,c('dma_key','month','year','lola_grp','seasonal_offset')]),by=c('dma_key','month','year','lola_grp'),all.x = T)


fin_data=merge(fin_data,data.table(mpd[,c('rtlr_num','cal_yr_mo_nbr','lola_grp','mpd')]),by=c('rtlr_num','cal_yr_mo_nbr','lola_grp'))


fin_data$store_fixed[is.na(fin_data$store_fixed)]=0
fin_data$recency_offset[is.na(fin_data$recency_offset)]=0
fin_data$recency_offset2[is.na(fin_data$recency_offset2)]=0

#fin_data=fin_data[is.na(segment),segment:='N.A']

fin_data=fin_data[is.na(regional_offset),regional_offset:=0]
fin_data=fin_data[is.na(seasonal_offset),seasonal_offset:=0]
#fin_data=fin_data[is.na(store_fixed),store_fixed:=0]
fin_data=fin_data[is.na(store_pres),store_pres:=0]


fin_data=fin_data[,ptr_med:=median(ptr, na.rm=T),by=.(dma_key,lola_grp,cal_yr_mo_nbr)]
fin_data=fin_data[,ptr_med2:=median(ptr, na.rm=T),by=.(state_cd,lola_grp,cal_yr_mo_nbr)]
fin_data=fin_data[,ptr_med3:=median(ptr, na.rm=T),by=.(dma_key,lola_grp)]
fin_data=fin_data[,ptr_med4:=median(ptr, na.rm=T),by=.(state_cd,lola_grp)]
fin_data=fin_data[ptr<1 | ptr>600,ptr:=ptr_med]
fin_data=fin_data[ptr<1 | ptr>600,ptr:=ptr_med2]
fin_data=fin_data[ptr<1 | ptr>600,ptr:=ptr_med3]
fin_data=fin_data[ptr<1 | ptr>600,ptr:=ptr_med4]
fin_data[,ptr_med:=NULL]
fin_data[,ptr_med2:=NULL]
fin_data[,ptr_med3:=NULL]
fin_data[,ptr_med4:=NULL]
fin_data=fin_data[ptr>0]
fin_data[,l_mpd:=log(mpd)]
fin_data[,l_ptr:=log(ptr)]


mod=randomForest(fin_data[,c(paste0('v_',5:304),'l_mpd','l_ptr','recency_offset','recency_offset2'
                             ,'regional_offset','seasonal_offset','store_fixed','store_pres')],fin_data$vol_sales,sampsize = 5000, nodesize = 8, ntree=75)

mod2=randomForest(fin_data[,c(paste0('v_',5:304))],fin_data$vol_sales,sampsize = 5000, nodesize = 8, ntree=75)

imp=data.frame(mod$importance)
imp$var_n=as.character(row.names(imp))

imp$IncNodePurity=imp$IncNodePurity/sum(imp$IncNodePurity)

names(var_map)[2]='var_n'
imp=merge(imp,var_map,by='var_n',all.x = T)

