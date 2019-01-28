rm(list= ls(all=TRUE))

library(data.table)
library(fastDummies)
library(randomForest)
library(DBI)
library(odbc)


con <- DBI::dbConnect(odbc::odbc(), driver = "SQL Server", dsn = "ZipAnalyticsADW", 
                      uid = "zipcode_analytics_app", pwd = "DECZr91@cF")


setwd('D:/harsh.vardhana/jan_fe/')

pbk1=fread('GBM_correctedconvSF_2019-01-22.csv')
pbk1$channel='convSF'
pbk2=fread('GBM_correctedpkgliqSF_2019-01-21.csv')
pbk2$channel='pkgliqSF'
pbk3=fread('GBM_correctedgrocLF_2019-01-22.csv')
pbk3$channel='grocLF'
pbk3$X=NULL
pkg_liq=rbind(pbk1,pbk2,pbk3)

#pkg_liq=fread('GBM_correctedpkgliqSF_2019-01-21.csv')

vol_dat1=fread(paste0('fin_vol_dat_pkgliqSF','.csv'))
vol_dat1$channel='pkgliqSF'
vol_dat2=fread(paste0('fin_vol_dat_convSF','.csv'))
vol_dat2$channel='convSF'
vol_dat3=fread(paste0('fin_vol_dat_grocLF','.csv'))
vol_dat3$channel='grocLF'
vol=rbind(vol_dat1,vol_dat2,vol_dat3)

off1=fread('offsets_pkgliqSF.csv')
off1$channel='pkgliqSF'
off2=fread('offsets_convSF.csv')
off2$channel='convSF'
off3=fread('offsets_grocLF.csv')
off3$channel='grocLF'
off=rbind(off1,off2,off3)


mpd=fread('mpd_month_offpremise.csv')

vol2=vol[cal_yr_mo_nbr %in% c(201811,201812) & vol_sales>0]
vol2=merge(vol2,mpd,by=c('rtlr_num','lola_grp','cal_yr_mo_nbr'))


#vol2=vol[cal_yr_mo_nbr %in% c(201811,201812),.(vol_sales=sum(vol_sales),mpd=sum(mpd)),by=.(rtlr_num,lola_grp)]
vol2=vol2[cal_yr_mo_nbr %in% c(201811,201812) & vol_sales>0,.(vol_sales=sum(vol_sales*ptr),mpd=sum(mpd)),by=.(rtlr_num,lola_grp)]
#vol2=merge(vol2,off[year==201901,.(rec=sum(recency_offset2 + recency_offset)),by=.(rtlr_num,lola_grp)],by=c('rtlr_num','lola_grp'),all.x=T)

pbk=dbGetQuery(con,"select rtlr_num, brand, store_increment from zip_analytics_test.playbook where channel in ('LIQUOR','GROCERY','MASS MERCH','CONVENIENCE') and dataset='mpd'")
  
pkg_liq=merge(pkg_liq,data.table(pbk)[,.(rtlr_num,lola_grp=brand,store_increment)],by=c('rtlr_num','lola_grp'),all.x=T)

pkg_liq[is.na(store_increment),store_increment:=0]

maint=data.table(data.frame(pkg_liq))

vol2[,ros:=vol_sales/mpd]
maint=merge(maint,vol2[,.(rtlr_num,lola_grp,ros)],by=c('rtlr_num','lola_grp'),all.x=T)
maint[is.na(ros),ros:=0]
maint=maint[current>0 | store_increment>0]



off=off[year==2019 & month==1,.(dma_key,lola_grp,channel,rec_off=recency_offset+recency_offset2)]
off=off[order(channel,dma_key,-rec_off)]
off=off[,head(.SD,3),by=.(channel,dma_key)]


off$ch=1
maint=merge(maint,off[,.(channel,dma_key,lola_grp,ch)],by=c('channel','dma_key','lola_grp'),all.x=T)
maint[is.na(ch),ch:=0]

maint=maint[order(rtlr_num,-(lift*store_increment),-ch,-ros)]
maint[,rank:=1]
maint[,rank:=cumsum(rank),by=.(rtlr_num)]
maint[,maint:=0]
maint[(rank <15 | rank==15) & store_increment==0,maint:=1]

mpd$channel=NULL
vol2=vol[cal_yr_mo_nbr %in% c(201811,201812) & vol_sales>0]
vol2=merge(vol2,mpd,by=c('rtlr_num','lola_grp','cal_yr_mo_nbr'))

vol2=vol2[cal_yr_mo_nbr %in% c(201811,201812),.(vol_sales=sum(vol_sales),mpd=sum(mpd)),by=.(dma_key,lola_grp,channel)]
vol2[,ros:=vol_sales/mpd]
vol2[,quant:=quantile(ros,0.2,na.rm = T),by=.(channel,dma_key)]

maint=merge(maint,unique(vol2[,.(channel,dma_key,quant)]),by=c('channel','dma_key'),all.x = T)
maint[ros<quant,maint:=0]

maint=maint[order(rtlr_num,-(lift*store_increment),-ch,-ros)]

# pbk=dbGetQuery(con,"select rtlr_num, brand, state_cd, store_initial_target from zip_analytics_test.playbook where channel='LIQUOR' and dataset='mpd' and store_increment=0")
# 
# 
# dd=maint[,.(dd=sum(maint)),by=.(lola_grp,state_cd)]
# dd2=data.table(pbk)[,.(dd2=length(rtlr_num)),by=.(brand,state_cd)]
# dd=merge(dd,dd2[,.(state_cd,lola_grp=(brand),dd2)],by=c('lola_grp','state_cd'),all=T)
# write.csv(dd,'maint_adj_rev.csv')


pbk=dbGetQuery(con,"select * from zip_analytics_test.playbook where channel in ('LIQUOR','GROCERY','MASS MERCH','CONVENIENCE') and dataset='mpd'")
pbk$ranking=as.numeric(pbk$ranking)
pbk$priority=as.numeric(pbk$priority)
pbk$dma_key=as.numeric(pbk$dma_key)
pbk$POD=as.numeric(pbk$POD)

maint_2=maint[maint==1,.(rtlr_num,brand=lola_grp,dma_key,state_cd,ranking=rank,store_increment,unconstrained_oppty=lift,unconstrained_increment=increment,unconstrained_target=target,cluster,POD=current)]
maint_2=merge(maint_2,unique(data.table(pbk)[,.(rtlr_num,wslr_nbr,channel,account_3.2)]),by='rtlr_num')
maint_2[,store_initial_oppty:=0]
maint_2[,store_initial_target:=POD]
maint_2[,model:='Base_co_maco']
maint_2[,dataset:='mpd']
maint_2[,variable:=paste0('mpd_',brand)]
maint_2[,sku_to_add_reco:='']

pbk=rbind(data.table(pbk)[store_increment>0],maint_2,fill=T)
pbk=pbk[order(channel,rtlr_num,ranking)]
pbk[,rank:=1]
pbk[,rank:=cumsum(rank),by=.(rtlr_num)]
pbk[,ranking:=rank]
pbk[,rank:=NULL]
fwrite(pbk,'off_prem_maint_adj.csv',row.names = F)

#######################on-premise###########################

rm(list= ls(all=TRUE))

library(data.table)
library(fastDummies)
library(randomForest)
library(DBI)
library(odbc)


con <- DBI::dbConnect(odbc::odbc(), driver = "SQL Server", dsn = "ZipAnalyticsADW", 
                      uid = "zipcode_analytics_app", pwd = "DECZr91@cF")


setwd('D:/harsh.vardhana/jan_fe/')

pbk1=fread('GBM_correctedbarONP_2019-01-22.csv')
pbk1$channel='barONP'
pbk2=fread('GBM_correctedresONP_2019-01-22.csv')
pbk2$channel='resONP'

pkg_liq=rbind(pbk1,pbk2)

#pkg_liq=fread('GBM_correctedpkgliqSF_2019-01-21.csv')

vol_dat1=fread(paste0('fin_vol_dat_barONP','.csv'))
vol_dat1$channel='barONP'
vol_dat2=fread(paste0('fin_vol_dat_resONP','.csv'))
vol_dat2$channel='resONP'

vol=rbind(vol_dat1,vol_dat2)

off1=fread('offsets_barONP.csv')
off1$channel='barONP'
off2=fread('offsets_resONP.csv')
off2$channel='resONP'
off=rbind(off1,off2)


mpd=fread('mpd_month_onpremise.csv')

vol2=vol[cal_yr_mo_nbr %in% c(201811,201812) & vol_sales>0]
vol2=merge(vol2,mpd,by=c('rtlr_num','lola_grp','cal_yr_mo_nbr'))


#vol2=vol[cal_yr_mo_nbr %in% c(201811,201812),.(vol_sales=sum(vol_sales),mpd=sum(mpd)),by=.(rtlr_num,lola_grp)]
vol2=vol2[cal_yr_mo_nbr %in% c(201811,201812) & vol_sales>0,.(vol_sales=sum(vol_sales*ptr),mpd=sum(mpd)),by=.(rtlr_num,lola_grp)]
#vol2=merge(vol2,off[year==201901,.(rec=sum(recency_offset2 + recency_offset)),by=.(rtlr_num,lola_grp)],by=c('rtlr_num','lola_grp'),all.x=T)

pbk=dbGetQuery(con,"select rtlr_num, brand, pack_draft, store_increment from zip_analytics_test.playbook where channel not in ('LIQUOR','GROCERY','MASS MERCH','CONVENIENCE') and dataset='mpd'")
pbk$brand=paste0(pbk$pack_draft,'_',pbk$brand)

pkg_liq=merge(pkg_liq,data.table(pbk)[,.(rtlr_num,lola_grp=brand,store_increment)],by=c('rtlr_num','lola_grp'),all.x=T)

pkg_liq[is.na(store_increment),store_increment:=0]

maint=data.table(data.frame(pkg_liq))

vol2[,ros:=vol_sales/mpd]
maint=merge(maint,vol2[,.(rtlr_num,lola_grp,ros)],by=c('rtlr_num','lola_grp'),all.x=T)
maint[is.na(ros),ros:=0]
maint=maint[current>0 | store_increment>0]



off=off[year==2019 & month==1,.(dma_key,lola_grp,channel,rec_off=recency_offset+recency_offset2)]
off=off[order(channel,dma_key,-rec_off)]
off=off[,head(.SD,3),by=.(channel,dma_key)]


off$ch=1
maint=merge(maint,off[,.(channel,dma_key,lola_grp,ch)],by=c('channel','dma_key','lola_grp'),all.x=T)
maint[is.na(ch),ch:=0]

maint=maint[order(rtlr_num,-(lift*store_increment),-ch,-ros)]
maint[,rank:=1]
maint[,rank:=cumsum(rank),by=.(rtlr_num)]
maint[,maint:=0]
maint[(rank <15 | rank==15) & store_increment==0,maint:=1]

mpd$channel=NULL
vol2=vol[cal_yr_mo_nbr %in% c(201811,201812) & vol_sales>0]
vol2=merge(vol2,mpd,by=c('rtlr_num','lola_grp','cal_yr_mo_nbr'))

vol2=vol2[cal_yr_mo_nbr %in% c(201811,201812),.(vol_sales=sum(vol_sales),mpd=sum(mpd)),by=.(dma_key,lola_grp,channel)]
vol2[,ros:=vol_sales/mpd]
vol2[,quant:=quantile(ros,0.2,na.rm = T),by=.(channel,dma_key)]

maint=merge(maint,unique(vol2[,.(channel,dma_key,quant)]),by=c('channel','dma_key'),all.x = T)
maint[ros<quant,maint:=0]

maint=maint[order(rtlr_num,-(lift*store_increment),-ch,-ros)]

# pbk=dbGetQuery(con,"select rtlr_num, brand, state_cd, store_initial_target from zip_analytics_test.playbook where channel='LIQUOR' and dataset='mpd' and store_increment=0")
# 
# 
# dd=maint[,.(dd=sum(maint)),by=.(lola_grp,state_cd)]
# dd2=data.table(pbk)[,.(dd2=length(rtlr_num)),by=.(brand,state_cd)]
# dd=merge(dd,dd2[,.(state_cd,lola_grp=(brand),dd2)],by=c('lola_grp','state_cd'),all=T)
# write.csv(dd,'maint_adj_rev.csv')


pbk=dbGetQuery(con,"select * from zip_analytics_test.playbook where channel not in ('LIQUOR','GROCERY','MASS MERCH','CONVENIENCE') and dataset='mpd'")
pbk$ranking=as.numeric(pbk$ranking)
pbk$priority=as.numeric(pbk$priority)
pbk$dma_key=as.numeric(pbk$dma_key)
pbk$POD=as.numeric(pbk$POD)

maint_2=maint[maint==1,.(rtlr_num,brand=lola_grp,dma_key,state_cd,ranking=rank,store_increment,unconstrained_oppty=lift,unconstrained_increment=increment,unconstrained_target=target,cluster,POD=current)]
maint_2=merge(maint_2,unique(data.table(pbk)[,.(rtlr_num,wslr_nbr,channel,account_3.2)]),by='rtlr_num')
maint_2[,store_initial_oppty:=0]
maint_2[,store_initial_target:=POD]
maint_2[,model:='Base_co_maco']
maint_2[,dataset:='mpd']
maint_2[,variable:=paste0('mpd_',brand)]
maint_2[,sku_to_add_reco:='']
maint_2[,pack_draft:=substr(brand,1,1)]
maint_2[,brand:=gsub('p_',"",brand)]
maint_2[,brand:=gsub('d_',"",brand)]

pbk=rbind(data.table(pbk)[store_increment>0],maint_2,fill=T)
pbk=pbk[order(channel,rtlr_num,ranking)]
pbk[,rank:=1]
pbk[,rank:=cumsum(rank),by=.(rtlr_num)]
pbk[,ranking:=rank]
pbk[,rank:=NULL]
fwrite(pbk,'on_prem_maint_adj.csv',row.names = F)
act=dbGetQuery(con,"select * from zip_analytics_test.playbook where dataset <> 'mpd'")
fwrite(act,'act_maint_adj.csv',row.names = F)

act=fread('act_maint_adj.csv')
off=fread('off_prem_maint_adj.csv')
on=fread('on_prem_maint_adj.csv')
act=act[order(rtlr_num,ranking)]



pbk=rbind(off,on,act)


fwrite(pbk,'final_pbk_maint_adj.csv',row.names = F,sep = '\t')
