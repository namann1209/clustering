rm(list= ls(all=TRUE))

library(data.table)
library(fastDummies)
library(DBI)
library(odbc)
library(randomForest)
#install.packages('fastDummies')
#channel1=c(convSF,grocLF,pkgliqSF,barONP,resONP)
#####DB Connection##########################################
con <- DBI::dbConnect(odbc::odbc(),
                      driver = "SQL Server",
                      dsn = "ZipAnalyticsADW",
                      uid = "zipcode_analytics_app",
                      pwd = "DECZr91@cF")

########################################################

setwd('D:/LOLA_FE_TEST/data/q4_refresh_modelling/jan_ref')


####################Arguments#############################
channel1='pkgliqSF'


rtlr_map=dbGetQuery(con,"select rtlr_num, rtlr_party_id, channel from zip_analytics_test.rtlr_geo_lookup where rtlr_party_id=rtlr_num")

ch_map=data.frame(c1=c('convSF','grocLF','pkgliqSF','barONP','resONP'),c2=c('CONVENIENCE','GROCERY','LIQUOR','BAR/TAVERN','RESTAURANT'))
channel2=as.character(ch_map$c2[ch_map$c1==channel1])
if(channel1 %in% c('barONP','resONP')){channel3='onpremise'}
if(!channel1 %in% c('barONP','resONP')){channel3='offpremise'}

############################################################
####################File Imports#############################



vol_dat=fread(paste0('fin_vol_dat_',channel1,'.csv'))
#  zip=fread(paste0('zip_share_',channel1,'.csv'))
mpd1=fread(paste0('mpd_month_',channel3,'.csv'))
mpd2=fread(paste0('mpd_month_',channel3,'_2.csv'))
offsets=fread(paste0('offsets_',channel1,'.csv'))
cluster_df=dbGetQuery(con,"select distinct rtlr_party_id rtlr_num, segment from zip_analytics_test.rtlr_num_clusters_without_adjustment_for_coli")
cluster_df$rtlr_num=as.integer(cluster_df$rtlr_num)
bud_df=read.csv('bud_df.csv')
iri_impte=fread('iri_imp_final.csv')
#zip=fread(paste0('zip_share_',channel1,'.csv'))

if(channel1=='convSF'){
  zip_dat = dbGetQuery(con,"select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and bi_channel='CONVENIENCE' group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier")}
if(channel1=='grocLF'){
  zip_dat = dbGetQuery(con,"select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and (bi_channel='GROCERY' or bi_channel='MASS MERCH') group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier")}
if(channel1=='pkgliqSF'){
  zip_dat = dbGetQuery(con,"select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and bi_channel='LIQUOR' group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier")}
if(channel1=='resONP'){
  zip_dat = dbGetQuery(con,"select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and bi_channel='RESTAURANT' group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier")}
if(channel1=='barONP'){
  zip_dat = dbGetQuery(con,"select zip, bi_channel channel, period cal_yr_mo_nbr, chain_ind, bisegmentdesc segment, supplier, sum(bbls) vol from zip_analytics_test.ab_zip_shr_data where period>201700 and bi_channel='BAR/TAVERN' group by zip, bi_channel, period, bisegmentdesc, chain_ind, supplier")}

zip=data.table(zip_dat)
zip1=zip[supplier=='ABI']
zip2=zip[supplier=='IND']
names(zip1)[names(zip1)=='vol']='abi'
names(zip2)[names(zip2)=='vol']='ind'
zip1[,supplier:=NULL]
zip2[,supplier:=NULL]
zip_f=merge(zip1,zip2,by=c('zip','cal_yr_mo_nbr','chain_ind','channel','segment'),all=T)
zip_f[is.na(zip_f)]=0
rm(zip1,zip2,zip_dat)
zip=data.frame(zip_f[,channel:=NULL])
rm(zip_f)

names(vol_dat)
names(mpd)
#names(ptr)
names(zip)
#names(trend_off)
names(offsets)

vol_dat=data.frame(vol_dat)
mpd=data.frame(mpd)
mpd_latest=data.frame(mpd_latest)
zip=data.frame(zip)
offsets=data.frame(offsets)
gc()

if(!channel1 %in% c('barONP','resONP')){
  segment_merge=dbGetQuery(con,"select prod_cd lola_grp,
                           case
                           when wamp_nm like '%WAMP5%' then 'CORE'
                           when wamp_nm like '%WAMP4%' then 'Premium'
                           when wamp_nm like '%Value%' then 'VALUE'
                           when wamp_nm like '%FMB%' then 'FMB'
                           when wamp_nm like '%Non Alc%' then 'N.A'
                           when wamp_nm like '%Super%' then 'H.E'
                           else 'Premium'
                           end as segment from zip_analytics_test.lola_brnd_prod_xref")


  
  segment_merge=data.table(segment_merge)
  segment_merge=segment_merge[,count:=1]
  segment_merge=segment_merge[,.(count=sum(count)),by=.(lola_grp,segment)]
  segment_merge[,cnt:=max(count),by=.(lola_grp)]
  segment_merge=segment_merge[cnt==count,.(lola_grp,segment)]
  segment_merge$lola_grp=tolower(segment_merge$lola_grp)}

############################################################
####################Functions#############################


#function to arrange columns in dataframe df according to the vector cols

column_arranger=function(df,cols){
  df=data.frame(df)
  df1=data.frame(crap=rep(1,dim(df)[1]))
  for(i in 1:length(cols)){
    if(!cols[i] %in% colnames(df)){df1$a=0}
    if(cols[i] %in% colnames(df)){df1$a=df[,cols[i]]}
    names(df1)[i+1]=cols[i]
  }
  df1$crap=NULL
  return(df1)
}


#####################Derived Argumetns####################################
lat_cal=vol_dat$latest_month_available[1]
sim_dates=c(vol_dat$simulation_start[1],vol_dat$simulation_start[1]+1,vol_dat$simulation_start[1]+2)
sim_dates[sim_dates-round(sim_dates/100,0)*100>12]=sim_dates[sim_dates-round(sim_dates/100,0)*100>12]+88


##################Dependent dataset Creation##########
vol_dat$zip=as.numeric(vol_dat$zip)
zip$zip=as.numeric(zip$zip)
zip$cal_yr_mo_nbr=as.numeric(zip$cal_yr_mo_nbr)

if(!channel1 %in% c('barONP','resONP')){
  vol_dat$segment=NULL
  
  vol_dat=merge(data.table(vol_dat),segment_merge,by='lola_grp')}

#Competition Share

#names(zip)[c(1,2,3,6,7)]
#[1] "zip"    "cal_yr_mo_nbr" "chain_ind"  "ind"  "abi"
industry_opt=data.table(zip[,c("zip","cal_yr_mo_nbr","chain_ind","ind","abi")])
industry_opt=industry_opt[,.(ind=sum(ind),abi=sum(abi)),by=.(zip,cal_yr_mo_nbr,chain_ind)]
industry_opt=industry_opt[,comp_share:= (ind-abi)/ind]
industry_opt=industry_opt[ind>0,]



#[1] "rtlr_num"      "cal_yr_mo_nbr" "lola_grp"      "vol_sales"
calib_frame=vol_dat[,c("rtlr_num","cal_yr_mo_nbr","lola_grp","vol_sales")]

#[1] "rtlr_num"      "cal_yr_mo_nbr" "zip"           "chain_ind"
industry_opt2=merge(data.table(unique(vol_dat[,c("rtlr_num","cal_yr_mo_nbr","zip","chain_ind")])),unique(industry_opt[,c('zip','chain_ind','cal_yr_mo_nbr','comp_share')]),by=c('zip','chain_ind','cal_yr_mo_nbr'))



calib_frame=merge(data.table(calib_frame),industry_opt2[,c('rtlr_num','cal_yr_mo_nbr','comp_share')],by=c('rtlr_num','cal_yr_mo_nbr'))
calib_frame=calib_frame[comp_share>0 & vol_sales>0,]
calib_frame=calib_frame[comp_share<1,]

calib_frame=calib_frame[,tot_vol:=sum(vol_sales),by=.(rtlr_num,cal_yr_mo_nbr)]
calib_frame=calib_frame[,share:=vol_sales/tot_vol]
calib_frame=calib_frame[,share:=share*(1-comp_share)]
calib_frame=calib_frame[,pseudo:=tot_vol/(1-comp_share)]

calib_frame=calib_frame[,const:=0]







st_dt=min(vol_dat$cal_yr_mo_nbr)+1
if((st_dt - round(st_dt/100,0)*100)>12){st_dt=st_dt-12;st_dt=st_dt+100}





reg_seg=data.table(zip)[,.(vol=sum(ind-abi)),by=.(zip,cal_yr_mo_nbr,chain_ind,segment)]
reg_seg=reshape(reg_seg,direction = 'wide',idvar = c('zip','cal_yr_mo_nbr','chain_ind'),timevar = 'segment')
reg_seg[reg_seg<0]=0
reg_seg[is.na(reg_seg)]=0
reg_seg=reg_seg[,vol.MAINSTREAM:=vol.MAINSTREAM+(vol.IMPORT/2)]
reg_seg=reg_seg[,vol.CRAFT:=vol.CRAFT+(vol.IMPORT/2)]
reg_seg=reg_seg[,vol.IMPORT:=NULL]
reg_seg=reg_seg[,tot:=vol.MAINSTREAM + vol.CRAFT + vol.ECONOMY + vol.FMB + `vol.NON-ALC`]
reg_seg=reg_seg[,vol.MAINSTREAM2:=median(vol.MAINSTREAM)];reg_seg=reg_seg[tot==0,vol.MAINSTREAM:=vol.MAINSTREAM2];reg_seg=reg_seg[,vol.MAINSTREAM2:=NULL]
reg_seg=reg_seg[,vol.MAINSTREAM2:=median(vol.CRAFT)];reg_seg=reg_seg[tot==0,vol.CRAFT:=vol.MAINSTREAM2];reg_seg=reg_seg[,vol.MAINSTREAM2:=NULL]
reg_seg=reg_seg[,vol.MAINSTREAM2:=median(vol.ECONOMY)];reg_seg=reg_seg[tot==0,vol.ECONOMY:=vol.MAINSTREAM2];reg_seg=reg_seg[,vol.MAINSTREAM2:=NULL]
reg_seg=reg_seg[,vol.MAINSTREAM2:=median(vol.FMB)];reg_seg=reg_seg[tot==0,vol.FMB:=vol.MAINSTREAM2];reg_seg=reg_seg[,vol.MAINSTREAM2:=NULL]
reg_seg=reg_seg[,vol.MAINSTREAM2:=median(`vol.NON-ALC`)];reg_seg=reg_seg[tot==0,`vol.NON-ALC`:=vol.MAINSTREAM2];reg_seg=reg_seg[,vol.MAINSTREAM2:=NULL]
reg_seg=reg_seg[,tot:=vol.MAINSTREAM + vol.CRAFT + vol.ECONOMY + vol.FMB + `vol.NON-ALC`]
reg_seg=reg_seg[,vol.MAINSTREAM:=vol.MAINSTREAM/tot]
reg_seg=reg_seg[,vol.CRAFT:=vol.CRAFT/tot]
reg_seg=reg_seg[,vol.ECONOMY:=vol.ECONOMY/tot]
reg_seg=reg_seg[,vol.FMB:=vol.FMB/tot]
reg_seg=reg_seg[,`vol.NON-ALC`:=`vol.NON-ALC`/tot]
reg_seg=reg_seg[,tot:=NULL]



if(channel1 %in% c('barONP','resONP')){
  ind_seg=data.table(zip)[,.(abi=sum(abi),ind=sum(ind)),by=.(zip,cal_yr_mo_nbr,chain_ind,segment)]
  ind_seg=ind_seg[abi<0,abi:=0]
  ind_seg=ind_seg[ind<0,ind:=0]
  ind_seg=ind_seg[,total:=sum(ind),by=.(zip,cal_yr_mo_nbr,chain_ind)]
  ind_seg=ind_seg[total>0]
  ind_seg=ind_seg[,comp:=ind-abi]
  ind_seg=ind_seg[comp<0,comp:=0]
  ind_seg=ind_seg[,comp_share:=comp/total]
  ind_seg=reshape(ind_seg[,.(zip,cal_yr_mo_nbr,chain_ind,segment,comp_share)],direction = 'wide',idvar = c('zip','cal_yr_mo_nbr','chain_ind'),timevar = 'segment')
  ind_seg[is.na(ind_seg)]=0
  ind_seg=ind_seg[comp_share.MAINSTREAM+comp_share.CRAFT!=0,comp_share.MAINSTREAM:=comp_share.MAINSTREAM+(comp_share.IMPORT*comp_share.MAINSTREAM/(comp_share.MAINSTREAM+comp_share.CRAFT))]
  ind_seg=ind_seg[comp_share.MAINSTREAM+comp_share.CRAFT!=0,comp_share.CRAFT:=comp_share.CRAFT+(comp_share.IMPORT*comp_share.CRAFT/(comp_share.MAINSTREAM+comp_share.CRAFT))]
  ind_seg=ind_seg[comp_share.MAINSTREAM+comp_share.CRAFT==0,comp_share.MAINSTREAM:=comp_share.MAINSTREAM+(comp_share.IMPORT/2)]
  ind_seg=ind_seg[comp_share.MAINSTREAM+comp_share.CRAFT==0,comp_share.CRAFT:=comp_share.CRAFT+(comp_share.IMPORT/2)]
  ind_seg=ind_seg[,comp_share.IMPORT:=NULL]
  
  ind_seg=reshape(ind_seg,direction='long',idvar = c('zip','cal_yr_mo_nbr','chain_ind'),varying = colnames(ind_seg)[4:8])
  names(ind_seg)[4]='segment'
  
  ind_seg=ind_seg[segment=='MAINSTREAM',segment:='Core']
  ind_seg=ind_seg[segment=='ECONOMY',segment:='Value']
  ind_seg=ind_seg[segment=='CRAFT',segment:='Premium']
  ind_seg=ind_seg[segment=='FMB',segment:='FMB']
  ind_seg=ind_seg[segment=='NON-ALC',segment:='N.A']}




dem=vol_dat[,c('rtlr_num','xcyfem','xcyea06v001','cyec17v001','xcya08v002')]
dem=data.table(dem)[,.(xcyfem=mean(xcyfem),xcyea06v001=mean(xcyea06v001),cyec17v001=mean(cyec17v001),xcya08v002=mean(xcya08v002)),by=.(rtlr_num)]




vol_dat1=data.table(vol_dat)
vol_dat1=merge(vol_dat1,mpd2[,c('rtlr_num','lola_grp','cal_yr_mo_nbr','mpd')],by=c('rtlr_num','lola_grp','cal_yr_mo_nbr'))
vol_dat1[vol_sales<0,vol_sales:=0]
ros=vol_dat1[,.(vol=sum(vol_sales),mpd=length(mpd)),by=.(dma_key,cal_yr_mo_nbr,lola_grp)]
ros=ros[,ros:=vol/mpd]
ros1=data.frame(ros)
ros1=data.table(ros1)[,cal_yr_mo_nbr:=cal_yr_mo_nbr+1]
ros1=ros1[cal_yr_mo_nbr-(round(cal_yr_mo_nbr/100,0)*100)==13,cal_yr_mo_nbr:=cal_yr_mo_nbr+88]
ros2=data.frame(ros1)
ros2=data.table(ros2)[,cal_yr_mo_nbr:=cal_yr_mo_nbr+1]
ros2=ros2[cal_yr_mo_nbr-(round(cal_yr_mo_nbr/100,0)*100)==13,cal_yr_mo_nbr:=cal_yr_mo_nbr+88]
names(ros1)[6]='ros1'
names(ros2)[6]='ros2'
ros=merge(ros,ros1[,.(dma_key,lola_grp,cal_yr_mo_nbr,ros1)],by=c('dma_key','lola_grp','cal_yr_mo_nbr'),all = T)
ros=merge(ros,ros2[,.(dma_key,lola_grp,cal_yr_mo_nbr,ros2)],by=c('dma_key','lola_grp','cal_yr_mo_nbr'),all = T)

ros$ros[is.na(ros$ros)]=0
ros$ros1[is.na(ros$ros1)]=0
ros$ros2[is.na(ros$ros2)]=0

ros$trend=ros$ros*3/(ros$ros+ros$ros1+ros$ros2)
ros$trend[ros$ros+ros$ros1+ros$ros2==0]=1
ros=data.table(ros)[,cal_yr_mo_nbr:=cal_yr_mo_nbr+1]
ros=ros[cal_yr_mo_nbr-(round(cal_yr_mo_nbr/100,0)*100)==13,cal_yr_mo_nbr:=cal_yr_mo_nbr+88]


intro=unique(data.table(vol_dat)[,.(rtlr_num,lola_grp,cal_yr_mo_nbr,vol_sales)])
intro1=data.frame(intro)
intro1=data.table(intro1)[,cal_yr_mo_nbr:=cal_yr_mo_nbr+1]
intro1=intro1[cal_yr_mo_nbr-(round(cal_yr_mo_nbr/100,0)*100)==13,cal_yr_mo_nbr:=cal_yr_mo_nbr+88]
intro2=data.frame(intro1)
intro2=data.table(intro2)[,cal_yr_mo_nbr:=cal_yr_mo_nbr+1]
intro2=intro2[cal_yr_mo_nbr-(round(cal_yr_mo_nbr/100,0)*100)==13,cal_yr_mo_nbr:=cal_yr_mo_nbr+88]
names(intro1)[4]='vol1'
names(intro2)[4]='vol2'

intro=merge(intro,intro1,by=c('rtlr_num','lola_grp','cal_yr_mo_nbr'),all = T)
intro=merge(intro,intro2,by=c('rtlr_num','lola_grp','cal_yr_mo_nbr'),all = T)


intro$vol1[is.na(intro$vol1)]=0
intro$vol2[is.na(intro$vol2)]=0
intro=intro[cal_yr_mo_nbr<(max(vol_dat$cal_yr_mo_nbr)+2)]
intro[,intro:=1]
intro[(vol1+vol2)>0,intro:=0]



time_s=unique(data.table(vol_dat)[cal_yr_mo_nbr>(st_dt+1),.(cal_yr_mo_nbr)])
time_s=time_s[order(cal_yr_mo_nbr)]
time_s$time=1:length(time_s$cal_yr_mo_nbr)
time_s=time_s[,time_vr:= time - mean(time)]
time_s=time_s[,time_vr2:= log(time) - mean(log(time))]


if(!channel1 %in% c('barONP','resONP')){
  seg_cols=c("segment","segment_CORE","segment_Premium","segment_VALUE","segment_FMB","segment_N.A","segment_H.E")}

if(channel1 %in% c('barONP','resONP')){
  seg_cols=c("segment","segment_Core","segment_Value","segment_FMB","segment_N.A","segment_Premium")}  

if(!channel1 %in% c('barONP','resONP')){iri_impte2=iri_impte[rtlr_num %in% unique(vol_dat$rtlr_num)]}

gc()
################Nest coeff#################################

nests=data.table(data.frame('segment'=unique(vol_dat$segment),'nest'=1:length(unique(vol_dat$segment))))

#par=refresh_nest_coeff() #Run quarterly to refresh nesting parameters
par=0.7
####post optimisation model run########################




calib_frame=data.table(vol_dat[vol_dat$vol_sales>0 & vol_dat$cal_yr_mo_nbr>201805,c('rtlr_num','cal_yr_mo_nbr','lola_grp','segment','vol_sales')])
calib_frame=calib_frame[,tot_vol:=sum(vol_sales),by=.(rtlr_num,cal_yr_mo_nbr)]
calib_frame=calib_frame[,share:=vol_sales/tot_vol]

if(!channel1 %in% c('barONP','resONP')){
  ind_tot=iri_impte2[,.(comp_share=sum(imp_comp_share)),by=.(rtlr_num,cal_yr_mo_nbr)]

  
  calib_frame=merge(calib_frame,ind_tot[,c('rtlr_num','cal_yr_mo_nbr','comp_share')],by=c('rtlr_num','cal_yr_mo_nbr'),sort = F)
  seg_rt=merge(iri_impte2,unique(calib_frame[,.(rtlr_num,cal_yr_mo_nbr)]),by=c('rtlr_num','cal_yr_mo_nbr'))
  calib_frame=calib_frame[,share:=share*((1-comp_share)/sum(share)),by=.(rtlr_num,cal_yr_mo_nbr)]
  
  seg_rt$lola_grp='comp'
  names(seg_rt)[4]='share'}

if(channel1 %in% c('barONP','resONP')){
  ind_tot=ind_seg[,.(comp_share=sum(comp_share)),by=.(zip,chain_ind,cal_yr_mo_nbr)]
  ind_tot=merge(ind_tot,unique(data.table(vol_dat)[cal_yr_mo_nbr>201805,c('zip','chain_ind','cal_yr_mo_nbr','rtlr_num')]),by=c('zip','chain_ind','cal_yr_mo_nbr'),allow.cartesian = TRUE,sort = F)
  seg_rt=merge(unique(vol_dat[vol_dat$cal_yr_mo_nbr>201805,c('zip','chain_ind','cal_yr_mo_nbr','rtlr_num')]),ind_seg,by=c('zip','chain_ind','cal_yr_mo_nbr'),allow.cartesian = TRUE,sort = F)
  
  calib_frame=merge(calib_frame,ind_tot[,c('rtlr_num','cal_yr_mo_nbr','comp_share')],by=c('rtlr_num','cal_yr_mo_nbr'),sort = F)
  
  calib_frame=calib_frame[,share:=share*((1-comp_share)/sum(share)),by=.(rtlr_num,cal_yr_mo_nbr)]
  
  seg_rt$lola_grp='comp'
  names(seg_rt)[6]='share'}

calib_frame=rbind(calib_frame,seg_rt[,c('rtlr_num','cal_yr_mo_nbr','lola_grp','segment','share')],fill=TRUE)

calib_frame=calib_frame[share<1 & share>0]

#nests=data.table(data.frame('segment'=unique(calib_frame$segment),'nest'=1:5))


calib_frame=merge(calib_frame,nests,by='segment',sort = F)

calib_frame=calib_frame[,c('rtlr_num','cal_yr_mo_nbr','lola_grp','share','nest')]


for(i in 1:length(nests$nest)){
  calib_frame=calib_frame[nest==i,iv:=par]}


calib_frame[,v:=0]
calib_frame[,num:=length(share),by=.(rtlr_num,cal_yr_mo_nbr,nest)]
calib_frame[num==1,iv:=1]


for(i in 1:200){
  calib_frame=calib_frame[,n_v:=sum(exp(v/iv)),by=.(rtlr_num,cal_yr_mo_nbr,nest)]
  calib_frame=calib_frame[,sh:=((exp(v/iv))*(n_v^(iv-1)))/sum((n_v^(iv))/num),by=.(rtlr_num,cal_yr_mo_nbr)]
  calib_frame=calib_frame[,v:=v+ log(share/sh)]
  print(mean(abs(calib_frame$sh - calib_frame$share)/calib_frame$share))
  if(mean(abs(calib_frame$sh - calib_frame$share)/calib_frame$share)<0.005){break}
}

load('pkg_mod.RData')
beta1=read.csv('pkg_reg.csv')
beta1=beta1$V1
month_c=read.csv('month_pkgliq.csv')
month_c=as.character(month_c$x)

########Compute bounds###########################  

latest_mpd=201811


  mpd=data.frame(mpd2[cal_yr_mo_nbr==latest_mpd,.(rtlr_num,lola_grp,cal_yr_mo_nbr,mpd)])
  prev_mpd=data.frame(mpd1[cal_yr_mo_nbr==latest_mpd-100,.(rtlr_num,lola_grp,cal_yr_mo_nbr,mpd)])
  last_yr_mpd=data.frame(mpd1[cal_yr_mo_nbr==201712,.(rtlr_num,lola_grp,cal_yr_mo_nbr,mpd)])



cluster_df=data.table(cluster_df)[,cnt:=length(segment),by=.(rtlr_num)]

cluster_df=cluster_df[cnt==1]

grwth_dates=dbGetQuery(con,"select distinct cal_yr_mo_nbr from zip_analytics_prod.dist_sold_new")
grwth_dates=grwth_dates[order(grwth_dates$cal_yr_mo_nbr),]
grwth_dates=grwth_dates[c(match(latest_mpd,grwth_dates)-1,match(latest_mpd,grwth_dates)-3)]

  grwth=data.frame(mpd1[cal_yr_mo_nbr %in% c(grwth_dates[1]-100,grwth_dates[2]-100),.(rtlr_num,lola_grp,cal_yr_mo_nbr,mpd)])



grwth=merge(data.table(grwth)[cal_yr_mo_nbr==grwth_dates[2]-100,.(rtlr_num,lola_grp,mpd)],data.table(grwth)[cal_yr_mo_nbr==grwth_dates[1]-100,.(rtlr_num,lola_grp,mpd_pr=mpd)],by=c('rtlr_num','lola_grp'),all=T)
grwth[is.na(grwth)]=0
grwth$lola_grp=tolower(as.character(grwth$lola_grp))
grwth$growth=grwth$mpd - grwth$mpd_pr

mpd$lola_grp=tolower(as.character(mpd$lola_grp))
prev_mpd$lola_grp=tolower(as.character(prev_mpd$lola_grp))
last_yr_mpd$lola_grp=tolower(as.character(last_yr_mpd$lola_grp))

#wk=max(mpd$iso_yr_wk_nbr)

mpd=data.table(mpd)[,.(mpd=sum(mpd/5)),by=.(rtlr_num,lola_grp)]
prev_mpd=data.table(prev_mpd)[,.(mpd=sum(mpd/5)),by=.(rtlr_num,lola_grp)]
last_yr_mpd$mpd=last_yr_mpd$mpd/4
last_yr_mpd=data.table(last_yr_mpd)[,.(mpd=sum(mpd)),by=.(rtlr_num,lola_grp)]


last_yr_mpd=merge(last_yr_mpd,prev_mpd[,.(lola_grp,rtlr_num,mpd_pr=mpd)],by=c('rtlr_num','lola_grp'),all=T)
#last_yr_mpd=merge(last_yr_mpd,rtlr_map[,c('rtlr_num','rtlr_party_id')],by='rtlr_party_id')
last_yr_mpd=merge(last_yr_mpd,unique(data.table(vol_dat)[,.(rtlr_num,state_cd)]),by='rtlr_num')
last_yr_mpd=merge(last_yr_mpd,unique(cluster_df[,c('rtlr_num','segment')]),by='rtlr_num',all.x = T)
names(last_yr_mpd)[names(last_yr_mpd)=='segment']='cluster'
last_yr_mpd=merge(last_yr_mpd,unique(data.table(vol_dat)[,.(lola_grp,segment)]),by='lola_grp',all.x = T)
last_yr_mpd[is.na(cluster),cluster:='No Cluster']
last_yr_mpd=merge(last_yr_mpd,grwth[,.(rtlr_num,lola_grp,growth)],by=c('rtlr_num','lola_grp'),all.x = T)
last_yr_mpd[is.na(last_yr_mpd)]=0


inp_dat=data.frame(dummy_cols(last_yr_mpd[,.(state_cd,cluster,lola_grp,segment)]))
inp_dat$mpd_pr=last_yr_mpd$mpd_pr
inp_dat$growth=last_yr_mpd$growth
inp_dat$state_cd=NULL
inp_dat$cluster=NULL
inp_dat$lola_grp=NULL
inp_dat$segment=NULL
out=last_yr_mpd$mpd - last_yr_mpd$mpd_pr

org_col=colnames(inp_dat)

mpd_mod=randomForest(inp_dat,out,sampsize = 7500, nodesize = 12, ntree=100)

names(mpd)[names(mpd)=='mpd']='mpd_pr'
#mpd=merge(mpd,rtlr_map[,c('rtlr_num','rtlr_party_id')],by='rtlr_party_id')
mpd=merge(mpd,unique(data.table(vol_dat)[,.(rtlr_num,state_cd)]),by='rtlr_num')
mpd=merge(mpd,unique(cluster_df[,c('rtlr_num','segment')]),by='rtlr_num',all.x = T)
names(mpd)[names(mpd)=='segment']='cluster'
mpd=merge(mpd,unique(data.table(vol_dat)[,.(lola_grp,segment)]),by='lola_grp',all.x = T)
mpd[is.na(cluster),cluster:='No Cluster']
mpd[is.na(mpd)]=0

if(channel3=='offpremise'){
  grwth=data.frame(mpd2[cal_yr_mo_nbr %in% c(grwth_dates[1],grwth_dates[2]),.(rtlr_num,lola_grp,cal_yr_mo_nbr,mpd)])
}


grwth=merge(data.table(grwth)[cal_yr_mo_nbr==grwth_dates[2],.(rtlr_num,lola_grp,mpd)],data.table(grwth)[cal_yr_mo_nbr==grwth_dates[1],.(rtlr_num,lola_grp,mpd_pr=mpd)],by=c('rtlr_num','lola_grp'),all=T)
grwth[is.na(grwth)]=0
grwth$lola_grp=tolower(as.character(grwth$lola_grp))
grwth$growth=grwth$mpd - grwth$mpd_pr

mpd=merge(mpd,grwth[,.(rtlr_num,lola_grp,growth)],by=c('rtlr_num','lola_grp'),all.x = T)
mpd[is.na(mpd)]=0


inp_dat=data.frame(dummy_cols(mpd[,.(state_cd,cluster,lola_grp,segment)]))
inp_dat$mpd_pr=mpd$mpd_pr
inp_dat$growth=mpd$growth
inp_dat$state_cd=NULL
inp_dat$cluster=NULL
inp_dat$lola_grp=NULL
inp_dat$segment=NULL

inp_dat=column_arranger(df=inp_dat,cols= org_col)

mpd$mpd=predict(mpd_mod,newdata = inp_dat)
mpd[,mpd:=mpd + mpd_pr]

mpd[mpd<0,mpd:=0]
mpd[,mpd:=round(mpd,0)]


rm(last_yr_mpd,prev_mpd,grwth,mpd_mod,inp_dat)

gc()

# [1] "rtlr_num"      "cal_yr_mo_nbr" "dma_key"     "lola_grp"   "state_cd"   "mpd"
bounds=data.table(vol_dat[vol_dat$cal_yr_mo_nbr %in% c(lat_cal,lat_cal-1,lat_cal-2),c('rtlr_num','dma_key','lola_grp','state_cd','cal_yr_mo_nbr')])
bounds=merge(bounds,mpd[,c('rtlr_num','lola_grp','mpd')],by=c('rtlr_num','lola_grp'))
bounds=bounds[,.(mpd=sum(mpd*4)),by=.(rtlr_num,dma_key,lola_grp,state_cd)]
id_count=dim(bounds)[2]-1
bounds=dcast(bounds,rtlr_num + dma_key + state_cd ~ lola_grp,value.var = 'mpd',sep='.')
l_groups=unique(colnames(bounds)[id_count:dim(bounds)[2]])

ref=c(1,id_count:dim(bounds)[2])
temp_b=data.frame(bounds)[,ref]

bounds[is.na(bounds)]=0
bud_df=data.table(bud_df)[,cnt:=length(bud_class),by=.(rtlr_num)]
cluster_df=data.table(cluster_df)[,cnt:=length(segment),by=.(rtlr_num)]

bud_df=bud_df[cnt==1]
cluster_df=cluster_df[cnt==1]
bounds=merge(bounds,bud_df[,.(rtlr_num,bud_class)],by='rtlr_num',all.x = TRUE)
bounds=merge(bounds,cluster_df[,.(rtlr_num,segment)],by='rtlr_num',all.x = TRUE)
bounds=bounds[,X:=NULL]
bounds=bounds[is.na(segment),segment:='No segment']
bounds$bud_class=as.character(bounds$bud_class)
bounds=bounds[bud_class!='D',bud_class:='BU']


#up_bound=unique(bounds[,.(rtlr_num)])
#l_groups=unique(colnames(bounds)[grep('mpd.',colnames(bounds))])



bound_calculator=function(x){return(quantile(x,0.80))}

t1=merge(bounds[,c('rtlr_num','state_cd','dma_key','segment','bud_class')],bounds[,lapply(.SD, bound_calculator),by=.(dma_key,segment,bud_class),.SDcols=l_groups],by=c('dma_key','segment','bud_class'),sort = F)
n1=merge(bounds[,c('rtlr_num','state_cd','dma_key','segment','bud_class')],bounds[,lapply(.SD, length),by=.(dma_key,segment,bud_class),.SDcols=l_groups],by=c('dma_key','segment','bud_class'),sort = F)

t2=merge(bounds[,c('rtlr_num','state_cd','dma_key','segment','bud_class')],bounds[,lapply(.SD, bound_calculator),by=.(state_cd,segment,bud_class),.SDcols=l_groups],by=c('state_cd','segment','bud_class'),sort = F)
n2=merge(bounds[,c('rtlr_num','state_cd','dma_key','segment','bud_class')],bounds[,lapply(.SD, length),by=.(state_cd,segment,bud_class),.SDcols=l_groups],by=c('state_cd','segment','bud_class'),sort = F)

t3=merge(bounds[,c('rtlr_num','state_cd','dma_key','segment','bud_class')],bounds[,lapply(.SD, bound_calculator),by=.(dma_key,bud_class),.SDcols=l_groups],by=c('dma_key','bud_class'),sort = F)


t1=as.matrix(data.frame(t1)[,l_groups])
n1=as.matrix(data.frame(n1)[,l_groups])
t2=as.matrix(data.frame(t2)[,l_groups])
n2=as.matrix(data.frame(n2)[,l_groups])
t3=as.matrix(data.frame(t3)[,l_groups])


temp_b[,l_groups]=t3
temp_b[,l_groups][n2>29]=t2[n2>29]
temp_b[,l_groups][n1>29]=t1[n1>29]

rm(t1,t2,t3,n1,n2,bounds)
gc()


up_bound=data.frame(melt(data.table(temp_b), id.vars = 'rtlr_num', measure.vars = l_groups))



names(up_bound)[2]='lola_grp'
names(up_bound)[3]='up'
up_bound$up[up_bound$up>5 & up_bound$up<12]=12
up_bound$up=round(up_bound$up,0)
if(channel2 %in% c('RESTAURANT','BAR/TAVERN')){
  fam_brnd=data.table(dbGetQuery(con,"select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref"))
  fam_brnd=fam_brnd[,.(count=length(brnd_cd)),by=.(lola_grp)]
  fam_brnd$lola_grp=tolower(as.character(fam_brnd$lola_grp))
  up_bound$up[up_bound$up>0][up_bound$lola_grp[up_bound$up>0] %in% fam_brnd$lola_grp[fam_brnd$count==1]]=12}

  
if(channel2 %in% c('LIQUOR')){  
rt_list=vol_dat[vol_dat$dma_key==751,c('rtlr_num')]
rt_list1=rt_list
rt_list3=rt_list
rt_list2=rt_list

rt_list$lola_grp='mpg'
rt_list1$lola_grp='muc'
rt_list2$lola_grp='mangomichelada'
rt_list3$lola_grp='goldenroadfamily'
rt_list=rbind(rt_list,rt_list1,rt_list2,rt_list3)
rt_list=rt_list[rt_list$rtlr_num %in% unique(up_bound$rtlr_num)]

chec=unique(up_bound[up_bound$up>0,c('rtlr_num','lola_grp')])
chec$d=1
rt_list=merge(rt_list,chec,by=c('rtlr_num','lola_grp'),all.x=T)
rt_list=rt_list[is.na(d),]
rt_list$d=NULL
rt_list$up=12

up_bound=rbind(up_bound,rt_list)

up_bound=data.table(up_bound)
up_bound=up_bound[,.(up=sum(up)),by=.(rtlr_num,lola_grp)]
}
###############simulation data creation#######################


sim_dat=merge(data.table(up_bound), mpd[,c('rtlr_num','lola_grp','mpd')],by=c('rtlr_num','lola_grp'),all.x=T)
sim_dat=sim_dat[!is.na(mpd)|up!=0]
sim_dat=sim_dat[is.na(mpd),mpd:=0]
sim_dat=sim_dat[,mpd:=mpd*4]
sim_dat=sim_dat[,up:=up/3]
sim_dat=merge(sim_dat,cluster_df[,c('rtlr_num','segment')],by='rtlr_num',all.x=T)
sim_dat=sim_dat[is.na(segment),segment:='No Cluster']

sim_dat=merge(sim_dat,unique(data.table(vol_dat[,c('rtlr_num','zip','chain_ind','dma_key','state_cd')])),by='rtlr_num')
sim_dat=merge(sim_dat,dem,by='rtlr_num',all.x=TRUE)

sim_dat_df=data.frame(sim_dat)

sim_dat=sim_dat[,cal_yr_mo_nbr:=sim_dates[1]]
sim_dat=rbind(sim_dat,sim_dat_df,fill=T)
sim_dat=sim_dat[is.na(cal_yr_mo_nbr),cal_yr_mo_nbr:=sim_dates[2]]
sim_dat=rbind(sim_dat,sim_dat_df,fill=T)
sim_dat=sim_dat[is.na(cal_yr_mo_nbr),cal_yr_mo_nbr:=sim_dates[3]]
sim_dat=sim_dat[,year:=round(cal_yr_mo_nbr/100,0)]
sim_dat=sim_dat[,month:=cal_yr_mo_nbr-(year*100)]

yr=round(sim_dates[1]/100,0)

sim_dat=merge(sim_dat,unique(offsets[offsets$month==sim_dates[1]-(yr*100),c('dma_key','year','lola_grp','recency_offset','recency_offset2')]),by=c('dma_key','year','lola_grp'),all.x = T)
sim_dat=merge(sim_dat,unique(offsets[!is.na(offsets$regional_offset),c('dma_key','year','lola_grp','regional_offset')]),by=c('dma_key','year','lola_grp'),all.x = T)
sim_dat=merge(sim_dat,unique(offsets[,c('dma_key','month','year','lola_grp','seasonal_offset')]),by=c('dma_key','month','year','lola_grp'),all.x = T)
sim_dat=sim_dat[is.na(regional_offset),regional_offset:=0]
sim_dat=sim_dat[is.na(seasonal_offset),seasonal_offset:=0]
sim_dat=sim_dat[is.na(recency_offset),recency_offset:=0]
sim_dat=sim_dat[is.na(recency_offset2),recency_offset2:=0]

names(sim_dat)[grep('segment',names(sim_dat))]='cluster'
#sim_dat=sim_dat[!lola_grp %in% c('bdo','mpg','nattyrushfamily','wickedweedfamily','nonalcbrands')]
sim_dat=merge(sim_dat,unique(data.table(vol_dat)[,.(lola_grp,segment)]),by='lola_grp')

sim_dat=merge(sim_dat,data.table(vol_dat)[cal_yr_mo_nbr %in% c(lat_cal,lat_cal-1,lat_cal-2),.(ptr=mean(ptr,na.rm = T)),by=.(rtlr_num,lola_grp)],by=c('rtlr_num','lola_grp'),all.x = TRUE)

sim_dat=merge(sim_dat,data.table(vol_dat)[cal_yr_mo_nbr %in% c(lat_cal,lat_cal-1,lat_cal-2),.(store_fixed=mean(store_fixed,na.rm = T),store_pres=mean(store_pres,na.rm = T)),by=.(rtlr_num,lola_grp)],by=c('rtlr_num','lola_grp'),all.x = TRUE)
sim_dat=sim_dat[is.na(store_fixed),store_fixed:=0]
sim_dat=sim_dat[is.na(store_pres),store_pres:=0]

sim_dat=sim_dat[,ptr_med:=median(ptr, na.rm=T),by=.(dma_key,lola_grp,cal_yr_mo_nbr)]
sim_dat=sim_dat[,ptr_med2:=median(ptr, na.rm=T),by=.(state_cd,lola_grp,cal_yr_mo_nbr)]
sim_dat=sim_dat[,ptr_med3:=median(ptr, na.rm=T),by=.(dma_key,lola_grp)]
sim_dat=sim_dat[,ptr_med4:=median(ptr, na.rm=T),by=.(state_cd,lola_grp)]
sim_dat=sim_dat[,ptr_med5:=median(ptr, na.rm=T),by=.(lola_grp)]
sim_dat=sim_dat[ptr<1 | ptr>600 | is.na(ptr),ptr:=ptr_med]
sim_dat=sim_dat[ptr<1 | ptr>600 | is.na(ptr),ptr:=ptr_med2]
sim_dat=sim_dat[ptr<1 | ptr>600 | is.na(ptr),ptr:=ptr_med3]
sim_dat=sim_dat[ptr<1 | ptr>600 | is.na(ptr),ptr:=ptr_med4]
sim_dat=sim_dat[ptr<1 | ptr>600 | is.na(ptr),ptr:=ptr_med5]
sim_dat[,ptr_med:=NULL]
sim_dat[,ptr_med2:=NULL]
sim_dat[,ptr_med3:=NULL]
sim_dat[,ptr_med4:=NULL]
sim_dat[,ptr_med5:=NULL]
sim_dat=sim_dat[ptr>0]


sim_dat=sim_dat[,marker:=1]
sim_dat=sim_dat[mpd==0,marker:=0]

a=mean(sim_dat$xcyea06v001,na.rm = T)
sim_dat[is.na(xcyea06v001),xcyea06v001:=a]
a=mean(sim_dat$cyec17v001,na.rm = T)
sim_dat[is.na(cyec17v001),cyec17v001:=a]
a=mean(sim_dat$xcya08v002,na.rm = T)
sim_dat[is.na(xcya08v002),xcya08v002:=a]
a=mean(sim_dat$xcyfem,na.rm = T)
sim_dat[is.na(xcyfem),xcyfem:=a]

zip_l=max(zip$cal_yr_mo_nbr)
sim_dat=merge(sim_dat,reg_seg[cal_yr_mo_nbr==zip_l,c("zip","chain_ind","vol.MAINSTREAM","vol.FMB","vol.NON-ALC","vol.CRAFT","vol.ECONOMY")],by=c('zip','chain_ind'),all.x = TRUE)


sim_dat=sim_dat[,reg_pres:=vol.MAINSTREAM]  
if(channel1 %in% c('barONP','resONP')){
  sim_dat=sim_dat[segment=='Value',reg_pres:=vol.ECONOMY]
  sim_dat=sim_dat[segment=='Premium',reg_pres:=vol.CRAFT]  }

if(!channel1 %in% c('barONP','resONP')){
  sim_dat=sim_dat[segment=='VALUE',reg_pres:=vol.ECONOMY]
  sim_dat=sim_dat[segment=='H.E',reg_pres:=vol.CRAFT]}  

sim_dat=sim_dat[segment=='FMB',reg_pres:=vol.FMB]
sim_dat=sim_dat[segment=='N.A',reg_pres:=`vol.NON-ALC`]
sim_dat[is.na(reg_pres),reg_pres:=0]


sim_dat=merge(sim_dat,nests,by='segment')

diff=sim_dates[1]-max(as.numeric(time_s$cal_yr_mo_nbr))
if (diff>11){diff=diff-88}

sim_dat$time[sim_dat$cal_yr_mo_nbr==sim_dates[1]]=max(time_s$time) + diff
sim_dat$time[sim_dat$cal_yr_mo_nbr==sim_dates[2]]=max(time_s$time) + diff +1
sim_dat$time[sim_dat$cal_yr_mo_nbr==sim_dates[3]]=max(time_s$time) + diff +2

rtlr_cal=data.table(vol_dat)[,.(cal_yr_mo_nbr=max(cal_yr_mo_nbr)),by=.(rtlr_num)]
rtlr_cal=merge(rtlr_cal,data.table(vol_dat)[,.(vol_sales=sum(vol_sales)),by=.(rtlr_num,cal_yr_mo_nbr)],by=c('cal_yr_mo_nbr','rtlr_num'))


sim_dat=merge(sim_dat,rtlr_cal[,c('rtlr_num','vol_sales')],by='rtlr_num')
sim_dat=sim_dat[vol_sales>0]
names(sim_dat)
if(channel2 %in% c('RESTAURANT','BAR/TAVERN')){sim_dat=merge(sim_dat,unique(vol_dat[,c('lola_grp','brand','drght_flg')]),by='lola_grp')}


if(channel2 %in% c('RESTAURANT','BAR/TAVERN')){l_grp=data.frame(dummy_cols(sim_dat[,.(brand)]));draft=(dummy_cols(data.frame(sim_dat[,'drght_flg'])))}
if(!channel2 %in% c('RESTAURANT','BAR/TAVERN')){l_grp=data.frame(dummy_cols(sim_dat[,.(lola_grp)]))}


seg=data.frame(dummy_cols(sim_dat[,.(segment)]))
state=data.frame(dummy_cols(data.frame(sim_dat[,.(state_cd)])))
month=data.frame(dummy_cols(sim_dat[,.(month=as.character(month))]))

seg=column_arranger(df = seg,cols = seg_cols)
#state=column_arranger(df = state,cols = state_c)



#l_grp=column_arranger(df = l_grp,cols = l_grp_c)
#seg=column_arranger(df = seg,cols = seg_c)
month=column_arranger(df = month,cols = month_c)

dt=lat_cal
sim_dat=merge(sim_dat,ros[cal_yr_mo_nbr==dt,.(dma_key,lola_grp,trend)],by=c('dma_key','lola_grp'),all.x = T,sort = F)
sim_dat=merge(sim_dat,intro[cal_yr_mo_nbr==dt,.(rtlr_num,lola_grp,intro)],by=c('rtlr_num','lola_grp'),all.x = T, sort = F)
sim_dat[is.na(trend),trend:=1]
sim_dat[is.na(intro),intro:=1]

#dd=colnames(x)

sim_dat[,mpd_vr:=log(mpd)]
sim_dat[,ptr_vr:=log(ptr)]
sim_dat[,reg_pres_vr:=reg_pres]
sim_dat[,seasonal_offset_vr:=seasonal_offset]
sim_dat[,trend_vr:=trend]
sim_dat[,intro_vr:=intro]
sim_dat[,time_vr2:=log(time)]

if(!channel2 %in% c('RESTAURANT','BAR/TAVERN')){
  # x=as.matrix(l_grp[,2:dim(l_grp)[2]])
  # x=cbind(x,as.matrix(seg[,2]*(st_grp1[,1:dim(st_grp1)[2]])),as.matrix(seg[,3]*st_grp2[,1:dim(st_grp2)[2]]),as.matrix(seg[,4]*st_grp3[,1:dim(st_grp3)[2]]),as.matrix(seg[,5]*st_grp4[,1:dim(st_grp4)[2]]),as.matrix(seg[,6]*st_grp5[,1:dim(st_grp5)[2]]))
  a1=as.matrix(cbind(as.matrix(seg[,2]*(month[,2:dim(month)[2]])),as.matrix(seg[,3]*month[,2:dim(month)[2]]),as.matrix(seg[,4]*month[,2:dim(month)[2]]),as.matrix(seg[,5]*month[,2:dim(month)[2]]),as.matrix(seg[,6]*month[,2:dim(month)[2]]),as.matrix(seg[,7]*month[,2:dim(month)[2]])))
  #x=cbind(x,as.matrix(log(sim_dat$cyec17v001 + 1)*seg[,2:6]),as.matrix(sim_dat$xcyea06v001*seg[,2:6]),as.matrix(sim_dat$cyec17v001*seg[,2:6]),as.matrix(sim_dat$xcyfem*seg[,2:6]))
  #  x1=cbind(x1,sim_dat$ptr_vr,as.matrix(sim_dat$mpd_vr*seg[,2:6]),sim_dat$trend_vr,sim_dat$seasonal_offset_vr,as.matrix(sim_dat$intro_vr*seg[,2:6]),as.matrix(sim_dat$time_vr2*seg[,2:6]))
}


if(channel2 %in% c('RESTAURANT','BAR/TAVERN')){
  # x=as.matrix(l_grp[,2:dim(l_grp)[2]])
  # x=cbind(x,as.matrix(draft[,2]),as.matrix(draft[,2]*(st_grp1[,1:dim(st_grp1)[2]])),as.matrix(seg[,2]*(st_grp2[,1:dim(st_grp2)[2]])),as.matrix(seg[,3]*st_grp3[,1:dim(st_grp3)[2]]),as.matrix(seg[,4]*st_grp4[,1:dim(st_grp4)[2]]),as.matrix(seg[,5]*st_grp5[,1:dim(st_grp5)[2]]),as.matrix(seg[,6]*st_grp6[,1:dim(st_grp6)[2]]))
  a1=as.matrix(cbind(as.matrix(seg[,2]*(month[,2:dim(month)[2]])),as.matrix(seg[,3]*month[,2:dim(month)[2]]),as.matrix(seg[,4]*month[,2:dim(month)[2]]),as.matrix(seg[,5]*month[,2:dim(month)[2]]),as.matrix(seg[,6]*month[,2:dim(month)[2]]),as.matrix(draft[,2]*month[,2:dim(month)[2]])))
  #x=cbind(x,as.matrix(log(sim_dat$cyec17v001 + 1)*seg[,2:6]),as.matrix(sim_dat$xcyea06v001*seg[,2:6]),as.matrix(sim_dat$cyec17v001*seg[,2:6]),as.matrix(sim_dat$xcyfem*seg[,2:6]))
  #  x1=cbind(x1,sim_dat$ptr_vr,sim_dat$mpd_vr*as.matrix(draft[,2]),sim_dat$mpd*as.matrix(seg[,2:6]),sim_dat$trend_vr,sim_dat$reg_pres_vr,sim_dat$seasonal_offset_vr,as.matrix(sim_dat$intro_vr*seg[,2:6]),as.matrix(sim_dat$time_vr2*seg[,2:6]))
}

if(!channel2 %in% c('RESTAURANT','BAR/TAVERN')){
  x=l_grp[,2:dim(l_grp)[2]]
  x=cbind(x,seg[,2:7],state[,2:dim(state)[2]],month[,2:dim(month)[2]])
  x=cbind(1,x,data.frame(sim_dat[,.(l_inc=log(cyec17v001 + 1),xcyea06v001,cyec17v001,xcyfem,store_fixed,store_pres,regional_offset,trend,time=log(time),recency_offset,recency_offset2,reg_pres)]))
}


if(channel2 %in% c('RESTAURANT','BAR/TAVERN')){
  x=l_grp[,2:dim(l_grp)[2]]
  x=cbind(x,draft[,2],seg[,2:6],state[,2:dim(state)[2]],month[,2:dim(month)[2]])
  x=cbind(1,x,data.frame(sim_dat[,.(l_inc=log(cyec17v001 + 1),xcyea06v001,cyec17v001,xcyfem,store_fixed,store_pres,regional_offset,trend,time=log(time),recency_offset,recency_offset2,reg_pres)]))  }


#par=st$par


calib_dat=sim_dat[,c('rtlr_num','lola_grp','nest','cal_yr_mo_nbr','vol_sales','marker')]

for(i in 1:length(nests$nest)){
  calib_dat=calib_dat[nest==i,iv:=par]}
calib_dat[,v:=0]

rtlr_cal=calib_frame[,.(cal_yr_mo_nbr=max(cal_yr_mo_nbr)),by=.(rtlr_num)]
rtlr_cal=merge(rtlr_cal,calib_frame[lola_grp=='comp',c('rtlr_num','lola_grp','nest','cal_yr_mo_nbr','v','iv')],by=c('rtlr_num','cal_yr_mo_nbr'))

rtlr_cal=rtlr_cal[,cal_yr_mo_nbr:=NULL]
sim_dat_df=data.frame(rtlr_cal)

rtlr_cal=rtlr_cal[,cal_yr_mo_nbr:=sim_dates[1]]
rtlr_cal=rbind(rtlr_cal,sim_dat_df,fill=T)
rtlr_cal=rtlr_cal[is.na(cal_yr_mo_nbr),cal_yr_mo_nbr:=sim_dates[2]]
rtlr_cal=rbind(rtlr_cal,sim_dat_df,fill=T)
rtlr_cal=rtlr_cal[is.na(cal_yr_mo_nbr),cal_yr_mo_nbr:=sim_dates[3]]



calib_dat=rbind(calib_dat,rtlr_cal[rtlr_num %in% unique(calib_dat$rtlr_num),c('rtlr_num','lola_grp','nest','iv','v','cal_yr_mo_nbr')],fill=T)
calib_dat=calib_dat[lola_grp=='comp',marker:=1]

calib_dat=calib_dat[,num:=sum(marker),by=.(rtlr_num,cal_yr_mo_nbr,nest)]

for(i in 1:length(nests$nest)){
  calib_dat=calib_dat[nest==i,iv:=par]}
calib_dat=calib_dat[num==1,iv:=1]

if(!channel2 %in% c('RESTAURANT','BAR/TAVERN')){
  x1=cbind(a1,sim_dat$ptr_vr,as.matrix(sim_dat$mpd_vr*seg[,2:7]),sim_dat$trend_vr,sim_dat$seasonal_offset_vr,as.matrix(sim_dat$intro_vr*seg[,2:7]),as.matrix(sim_dat$time_vr2*seg[,2:7]))
}

if(channel2 %in% c('RESTAURANT','BAR/TAVERN')){
  x1=cbind(a1,sim_dat$ptr_vr,sim_dat$mpd_vr*as.matrix(draft[,2]),sim_dat$mpd_vr*as.matrix(seg[,2:6]),sim_dat$trend_vr,sim_dat$reg_pres_vr,sim_dat$seasonal_offset_vr,as.matrix(sim_dat$intro_vr*seg[,2:6]),as.matrix(sim_dat$time_vr2*seg[,2:6]))
}


cols_reqd=row.names(mod$importance)

for(i in 1:length(cols_reqd)){
  if(!cols_reqd[i] %in% colnames(x)){
  x$a=0
  names(x)[names(x)=='a']=cols_reqd[i]
    }
  
}

fixed_comp=predict(mod,newdata = x)

calib_dat=calib_dat[lola_grp!='comp',v:= fixed_comp + x1%*%beta1]
calib_dat=calib_dat[is.na(v),v:=0]

oo=quantile(calib_frame$v[calib_frame$lola_grp=='comp'],0.25)

calib_dat=calib_dat[,n_v:=sum(marker*exp(v/iv)),by=.(rtlr_num,cal_yr_mo_nbr,nest)]
calib_dat=calib_dat[marker==1,sh:=((marker*exp(v/iv))*(n_v^(iv-1)))/(sum(((n_v^(iv))/num)) + exp(oo)),by=.(rtlr_num,cal_yr_mo_nbr)]
calib_dat=calib_dat[marker==0,sh:=0]

cc=calib_dat[lola_grp!='comp',.(ab_sh=sum(sh)),by=.(rtlr_num,cal_yr_mo_nbr)]
calib_dat=calib_dat[lola_grp!='comp',ab_sh:=sum(sh),by=.(rtlr_num,cal_yr_mo_nbr)]
calib_dat=calib_dat[lola_grp!='comp',pseudo:=vol_sales/ab_sh]
calib_dat=calib_dat[lola_grp!='comp',org_vol:=pseudo*sh]
sim_dat=sim_dat[,mpd_org:=mpd]



lola_groups=unique(sim_dat$lola_grp)
for(i in 1:length(lola_groups)){
  sim_dat=sim_dat[,mpd:=mpd_org]
  sim_dat=sim_dat[lola_grp==lola_groups[i] & (mpd+4<up | mpd+4==up),mpd:=mpd+4]
  sim_dat=sim_dat[lola_grp==lola_groups[i] & (mpd+4<up | mpd+4==up),mpd:=mpd+4]
  sim_dat=sim_dat[lola_grp==lola_groups[i] & (mpd+4<up | mpd+4==up),mpd:=mpd+4]
  sim_dat=sim_dat[,marker:=1]
  sim_dat=sim_dat[mpd==0,marker:=0]
  
  sim_dat[,mpd_vr:=log(mpd)]
  
  if(!channel2 %in% c('RESTAURANT','BAR/TAVERN')){
    x1=cbind(a1,sim_dat$ptr_vr,as.matrix(sim_dat$mpd_vr*seg[,2:7]),sim_dat$trend_vr,sim_dat$seasonal_offset_vr,as.matrix(sim_dat$intro_vr*seg[,2:7]),as.matrix(sim_dat$time_vr2*seg[,2:7]))
  }
  
  if(channel2 %in% c('RESTAURANT','BAR/TAVERN')){
    x1=cbind(a1,sim_dat$ptr_vr,sim_dat$mpd_vr*as.matrix(draft[,2]),sim_dat$mpd_vr*as.matrix(seg[,2:6]),sim_dat$trend_vr,sim_dat$reg_pres_vr,sim_dat$seasonal_offset_vr,as.matrix(sim_dat$intro_vr*seg[,2:6]),as.matrix(sim_dat$time_vr2*seg[,2:6]))
  }
  
  
  calib_dat=calib_dat[lola_grp!='comp',v:= fixed_comp + x1%*%beta1]
  calib_dat=calib_dat[is.na(v),v:=0]
  calib_dat=calib_dat[lola_grp!='comp',marker:= sim_dat$marker]
  calib_dat=calib_dat[,num:=sum(marker),by=.(rtlr_num,cal_yr_mo_nbr,nest)]
  
  for(j in 1:length(nests$nest)){
    calib_dat=calib_dat[nest==j,iv:=par]}
  calib_dat=calib_dat[num==1,iv:=1]
  calib_dat=calib_dat[,n_v:=sum(marker*exp(v/iv)),by=.(rtlr_num,cal_yr_mo_nbr,nest)]
  calib_dat=calib_dat[marker==1,pr_sh:=((marker*exp(v/iv))*(n_v^(iv-1)))/(sum((n_v^(iv))/num)+exp(oo)),by=.(rtlr_num,cal_yr_mo_nbr)]
  calib_dat=calib_dat[marker==0,pr_sh:=0]
  
  calib_dat=calib_dat[lola_grp!='comp',pr_vol:=pseudo*pr_sh]
  if(i==1){lift=calib_dat[lola_grp!='comp',.(lift=sum(pr_vol-org_vol)),by=.(rtlr_num)];lift[,lola_grp:=lola_groups[i]]}
  if(i>1){lift2=calib_dat[lola_grp!='comp',.(lift=sum(pr_vol-org_vol)),by=.(rtlr_num)];lift2[,lola_grp:=lola_groups[i]];lift=rbind(lift,lift2)}
  #  if(i==1){rec_vol=calib_dat[lola_grp!='comp',.(n_vol=pr_vol)];names(rec_vol)[i]=paste0(lola_groups[i],"_vol")}
  #  if(i>1){rec_vol=cbind(rec_vol,calib_dat[lola_grp!='comp',.(n_vol=pr_vol)]);names(rec_vol)[i]=paste0(lola_groups[i],"_vol")}
  #  if(i==1){rec_mpd=calib_dat[lola_grp!='comp',.(n_mpd=sim_dat$mpd)];names(rec_mpd)[i]=paste0(lola_groups[i],"_mpd")}
  #  if(i>1){rec_mpd=cbind(rec_mpd,calib_dat[lola_grp!='`comp',.(n_mpd=sim_dat$mpd)]);names(rec_mpd)[i]=paste0(lola_groups[i],"_mpd")}
}

lift=merge(lift,unique(data.table(vol_dat)[,.(rtlr_num,dma_key)]),by='rtlr_num')
lift=merge(lift,unique(data.table(vol_dat)[,.(lola_grp,segment)]),by='lola_grp')
names(cluster_df)[names(cluster_df)=='segment']='cluster'
lift=merge(lift,unique(cluster_df[,c('rtlr_num','cluster')]),by='rtlr_num',all.x = TRUE)
lift=lift[is.na(cluster),cluster:='No clutser']
lift=lift[!is.na(lift)]

lift[,.(vol_gain=sum(lift)),by=.(segment)]

lola_groups=unique(sim_dat$lola_grp)
for(i in 1:length(unique(sim_dat$lola_grp))){
  sim_dat=sim_dat[,mpd:=mpd_org]
  sim_dat=sim_dat[lola_grp==lola_groups[i] & (mpd+4<up | mpd+4==up),mpd:=mpd+4]
  sim_dat=sim_dat[lola_grp==lola_groups[i] & (mpd+4<up | mpd+4==up),mpd:=mpd+4]
  sim_dat=sim_dat[lola_grp==lola_groups[i] & (mpd+4<up | mpd+4==up),mpd:=mpd+4]
  if(i==1){target=sim_dat[lola_grp==lola_groups[i],.(rtlr_num=rtlr_num,target=mpd,current=mpd_org,lola_grp=lola_grp)]}
  if(i>1){target=rbind(target,sim_dat[lola_grp==lola_groups[i],.(rtlr_num=rtlr_num,target=mpd,current=mpd_org,lola_grp=lola_grp)])}
}


lift=merge(lift,unique(target),by=c('rtlr_num','lola_grp'),all.x = TRUE)
lift=lift[!is.na(target),]
lift=merge(lift,sim_dat[ptr<600,.(ptr_m=mean(ptr)),by=.(lola_grp)],by='lola_grp')
lift=lift[,increment:=(target-current)/4]
div=c(0.8,0.7)
dma_l=lift[,.(mpd=sum(increment),lift=sum(lift*ptr_m)),by=.(dma_key)]
dma_l=dma_l[,l_mpd:=lift/mpd]
dma_l=dma_l[order(l_mpd)]
dma_l$perc=c(1:length(dma_l$mpd))/length(dma_l$mpd)

st_l=lift[,.(mpd=sum(increment),lift=sum(lift*ptr_m)),by=.(rtlr_num)]
st_l=st_l[,l_mpd:=lift/mpd]
st_l=st_l[order(l_mpd)]
st_l$st_perc=c(1:length(st_l$mpd))/length(st_l$mpd)

lift=merge(lift,dma_l[,.(dma_key,perc)],by='dma_key')
lift=merge(lift,st_l[,.(rtlr_num,st_perc)],by='rtlr_num')

lift=lift[,cut:=0.50]
lift=lift[((perc+st_perc)/2)>0.25,cut:=0.35]
lift=lift[((perc+st_perc)/2)>0.75,cut:=0.2]

lift=lift[,inc_rev:=ptr_m*lift]
lift=lift[,inc_mpd:=0]
lift=lift[increment>0,inc_mpd:=inc_rev/increment]


lift=lift[order(rtlr_num,inc_mpd)]

lift=lift[,cum_count:=cumsum(increment),by=.(rtlr_num)]
lift=lift[,tot_count:=sum(increment),by=.(rtlr_num)]
lift=lift[,to_shave:=cut*tot_count]
lift=lift[,to_sub:=(cut*tot_count)/2]
# lift=lift[cum_count<to_shave,increment:=0]
# lift=lift[cum_count<to_sub,increment:=-1]
# lift=lift[increment==0,target:=current]
# lift=lift[increment==-1,target:=current]
# lift=lift[increment==0,lift:=0]
# lift=lift[increment==-1,lift:=-lift]

lift=merge(lift,unique(vol_dat[,c('rtlr_num','state_cd')]),by='rtlr_num')
lift=merge(lift,unique(vol_dat[,c('rtlr_num','zip')]),by='rtlr_num')

fwrite(lift,paste0('GBM_corrected',channel1,'_',Sys.Date(),'.csv'),row.names = FALSE,sep=",")
