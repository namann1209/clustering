rm(list= ls(all=TRUE))

library(data.table)
library(fastDummies)
library(randomForest)
library(DBI)
library(odbc)


con <- DBI::dbConnect(odbc::odbc(), driver = "SQL Server", dsn = "ZipAnalyticsADW", 
                      uid = "zipcode_analytics_app", pwd = "DECZr91@cF")


setwd('D:/harsh.vardhana/jan_fe')

#Apply trend constraint or not
trend_constraint='Yes'
#choose optimization parameter
by='revenue';share='no';split=c(0.5,0.5)

pbk1=fread('GBM_correctedconvSF_2018-12-23.csv')
pbk1$channel='convSF'
pbk2=fread('GBM_correctedpkgliqSF_2018-12-23.csv')
pbk2$channel='pkgliqSF'
pbk3=fread('GBM_correctedgrocLF_2018-12-23.csv')
pbk3$channel='grocLF'
pbk3$X=NULL
pbk=rbind(pbk1,pbk2,pbk3)

if(trend_constraint=='Yes'){
vol_dat1=fread(paste0('fin_vol_dat_pkgliqSF','.csv'))
vol_dat1$channel='pkgliqSF'
vol_dat2=fread(paste0('fin_vol_dat_convSF','.csv'))
vol_dat2$channel='convSF'
vol_dat3=fread(paste0('fin_vol_dat_grocLF','.csv'))
vol_dat3$channel='grocLF'
vol_dat=rbind(vol_dat1,vol_dat2,vol_dat3)

lat_dat=as.numeric(vol_dat$latest_month_available[1])
denv_excep=pbk[state_cd =='CO' & channel=='pkgliqSF' & dma_key==751 & lola_grp %in% c('mangomichelada','goldenroadfamily','muc','mpg')]
denv_excep[increment>0,lif_inc:=lift/increment]
denv_excep[increment==0,lif_inc:=0]
q1=quantile(denv_excep$lif_inc[denv_excep$increment>0],0.25)
q2=quantile(denv_excep$lif_inc[denv_excep$increment>0],0.75)
denv_excep[lif_inc<q2,increment:=2]
denv_excep[lif_inc<q1,increment:=1]
denv_excep[,target:= current + (increment*4)]
denv_excep[,lif_inc:=NULL]


down=lat_dat - 6
if(down - round(down/100)*100>12){down=down-88}
annual=vol_dat[cal_yr_mo_nbr<(lat_dat + 1) & cal_yr_mo_nbr>down,.(sales_lat=sum(vol_sales)),by=.(channel,dma_key,lola_grp)]
annual=merge(annual,vol_dat[cal_yr_mo_nbr<(lat_dat - 99) & cal_yr_mo_nbr>(down - 100),.(sales_pre=sum(vol_sales)),by=.(channel,dma_key,lola_grp)],by=c('channel','dma_key','lola_grp'),all.x=TRUE)
annual[is.na(annual)]=0
annual=annual[,long_trend:=(sales_lat - sales_pre)/(sales_pre+0.1)]
annual=annual[order(channel,dma_key,-long_trend)]
annual[,elim:=1]
annual[,elim:=cumsum(elim),by=.(channel,dma_key)]
annual[,total:=length(lola_grp),by=.(channel,dma_key)]
annual[,mark:=0]
annual[elim>0.75*total,mark:=1]
annual[elim>0.90*total,mark:=2]

down1=lat_dat - 1
if(down1 - (round(down1/100))*100>12){down1=down1-88}
down=lat_dat - 2
if(down - round(down/100)*100>12){down=down-88}
down2=lat_dat - 4
if(down2 - round(down2/100)*100>12){down2=down2-88}
short=vol_dat[cal_yr_mo_nbr<(lat_dat + 1) & cal_yr_mo_nbr>down,.(sales_lat=sum(vol_sales)),by=.(channel,dma_key,lola_grp)]
short=merge(short,vol_dat[cal_yr_mo_nbr<down1 & cal_yr_mo_nbr>down2,.(sales_pre=sum(vol_sales)),by=.(channel,dma_key,lola_grp)],by=c('channel','dma_key','lola_grp'),all=TRUE)
short[is.na(short)]=0
short=short[,long_trend:=(sales_lat - sales_pre)/(sales_pre+0.1)]
short=short[order(channel,dma_key,-long_trend)]
short[,elim:=1]
short[,elim:=cumsum(elim),by=.(channel,dma_key)]
short[,total:=length(lola_grp),by=.(channel,dma_key)]
short[,mark_short:=0]
short[elim>0.74*total,mark_short:=1]
short[elim>0.90*total,mark_short:=2]


pbk=data.table(pbk)
pbk=merge(pbk,annual[,.(channel,dma_key,lola_grp,mark)],by=c('channel','dma_key','lola_grp'))
pbk=merge(pbk,short[,.(channel,dma_key,lola_grp,mark_short)],by=c('channel','dma_key','lola_grp'))


pbk[mark>0,lift:=0]
pbk[mark>0,increment:=0]
pbk[mark>0,target:=current]

# pbk[mark_short>0,lift:=0]
# pbk[mark_short>0,increment:=0]
# pbk[mark_short>0,target:=current]
}
pbk=pbk[!(state_cd =='CO' & channel=='pkgliqSF' & dma_key==751 & lola_grp %in% c('mangomichelada','goldenroadfamily','muc','mpg'))]


pbk[,d_marker:=0]
denv_excep[,d_marker:=1]

pbk=rbind(pbk,denv_excep,fill=T)

maco=dbGetQuery(con,"select a.wslr_nbr, b.lola_grp, sum(a.vol_bbls_qty) vol, sum(a.VARBL_MRGN_AMT - a.TFER_EXPNS_AMT) prof  from (select wslr_nbr, brnd_cd, vol_bbls_qty, VARBL_MRGN_AMT, TFER_EXPNS_AMT from zip_analytics_test.maco_wslr_cust where cal_yr_mo_nbr>201803 and vol_bbls_qty>0) as a inner join (select brnd_cd, prod_cd lola_grp from zip_analytics_test.lola_brnd_prod_xref) as b on a.brnd_cd=b.brnd_cd group by a.wslr_nbr, b.lola_grp")

maco$maco=maco$prof*1000/maco$vol
maco=data.table(maco)
maco$lola_grp=tolower(maco$lola_grp)

brnd_carried=read.csv('wslr_carried_brands_2.csv')
rtlr_map=read.csv('rtlr_wslr.csv')
space=read.csv('space_const.csv')
license=read.csv('rtlr_licence_mapping.csv')
brnd_allowed=read.csv('licence_brnd_mapping.csv')

brnd_carried$lola_grp=as.character(brnd_carried$lola_grp)
brnd_carried$lola_grp=tolower(as.character(brnd_carried$lola_grp))



pbk=data.table(pbk)
pbk$lola_grp=as.character(pbk$lola_grp)
pbk=merge(pbk,rtlr_map,by='rtlr_num')
pbk=merge(pbk,brnd_carried,by=c('wslr_nbr','lola_grp'),all.x = T)
pbk[is.na(PKG_COUNT),PKG_COUNT:=0]
pbk=pbk[,target:=target/4]
pbk=pbk[,current:=current/4]

pbk=pbk[,target2:=target]
pbk[PKG_COUNT<target & d_marker==0,target2:=PKG_COUNT]
pbk[PKG_COUNT<target & PKG_COUNT < current & d_marker==0,target2:=current]


pbk=pbk[,lift2:=lift]
pbk=pbk[target>current,lift2:=lift*(target2-current)/(target-current)]

pbk=pbk[,increment2:=target2-current]

pbk=pbk[,inc_rev:=lift2*ptr_m]



license=merge(license,pbk[,.(exc=max(d_marker)),by=.(rtlr_num)],by='rtlr_num',all.x=T)
license$licence[license$exc==1]=0

brnd_allowed$lola_grp=tolower(as.character(brnd_allowed$lola_grp))
pbk=merge(pbk,license,by='rtlr_num',all.x = T)
pbk=pbk[is.na(licence),licence:=0]
pbk=merge(pbk,brnd_allowed,by='lola_grp',all.x = T)
pbk[is.na(cnt),cnt:=0]


pbk=pbk[,target3:=target2]
pbk[cnt<target2 & licence==1,target3:=cnt]
pbk[cnt<target2 & cnt < current & licence==1,target3:=current]

pbk=pbk[,lift3:=lift2]
pbk=pbk[target2>current,lift3:=lift*(target3-current)/(target2-current)]

pbk=pbk[,increment3:=target3-current]
maco$wslr_nbr=as.integer(maco$wslr_nbr)


pbk=merge(pbk,maco[,.(wslr_nbr,lola_grp,maco)],by=c('wslr_nbr','lola_grp'),all.x = T)
pbk[,m_med:=median(maco,na.rm = T),by=.(lola_grp,state_cd)]
pbk[maco<17,maco:=m_med]
pbk[maco>200,maco:=m_med]
pbk[,m_med:=median(maco,na.rm = T),by=.(lola_grp)]
pbk[maco<17,maco:=m_med]
pbk[maco>200,maco:=m_med]
pbk=pbk[is.na(maco),maco:=0]

#pbk=pbk[increment3==0 | maco>0]

pbk[,exc:=sum(current),by=.(rtlr_num)]
pbk[,revn:=ptr_m]

const_opt=function(by='volume',share='no',split=c(0.5,0.5)){
  
  if (share=='no'){  
    if (by=='volume'){
      pbk[,ptr_m:=1]}
    if (by=='revenue'){
      pbk[,ptr_m:=ptr_m]}
    if (by=='maco'){
      pbk[,ptr_m:=maco]}}
  
  
  if (share=='yes'){  
    pbk=pbk[,ptr_t:=0]
    if('revenue' %in% by){pbk[,ptr_t:=ptr_t + ptr_m*split[match('revenue',by)]]}
    if('maco' %in% by){pbk[,ptr_t:=ptr_t + maco*split[match('maco',by)]]}
    if('volume' %in% by){pbk[,ptr_t:=ptr_t + split[match('volume',by)]]}
    pbk=pbk[,ptr_m:=ptr_t]
    pbk=pbk[,ptr_t:=NULL]
  }
  
  
  pbk[lift3==0,increment3:=0]
  pbk[lift3==0,target3:=current]
  pbk[increment3==0,ptr_m:=0]
  
  pbk=pbk[,inc_rev:=lift3*ptr_m]
  pbk=pbk[increment3>0,inc_mpd:=inc_rev/increment3]
  pbk=pbk[increment3==0,inc_mpd:=0]
  pbk=pbk[,brnd_lift:=sum(inc_mpd),by=.(channel,lola_grp,dma_key)]
  pbk=pbk[,tot_lift:=sum(inc_mpd),by=.(channel,dma_key)]
  pbk=pbk[,lift_share:=brnd_lift/tot_lift]
  pbk=pbk[tot_lift==0,lift_share:=0]
  
  
  pbk=merge(pbk,space,by='rtlr_num',all.x = T)
  
  pbk[,intro:=0]
  pbk[current==0,intro:=1]
  
  pbk=pbk[,store_total:=sum(current),by=.(rtlr_num)]
  pbk=pbk[,store_total_new:=sum(target3),by=.(rtlr_num)]
  
  pbk=merge(pbk,pbk[current!=0,.(brands_in_store=length(lola_grp)),by=.(rtlr_num)],by='rtlr_num',all.x = T)
  
  pbk_int=pbk[current==0]
  pbk_inc=pbk[current>0]
  
  pbk_sp=pbk_int[!is.na(SHLF_TOTAL) & exc>0]
  
  pbk_sp=pbk_sp[,mpd_space:=SHLF_TOTAL/brands_in_store]
  pbk_sp=pbk_sp[,opp:=sum(lift3*ptr_m)/sum(increment3),by=.(rtlr_num)]
  pbk_sp=pbk_sp[,tot_inc:=sum(increment3),by=.(rtlr_num)]
  pbk_sp=pbk_sp[tot_inc==0,opp:=0]
  
  
  quant_calc=unique(pbk_sp[,.(rtlr_num,cluster,state_cd,mpd_space,opp,channel)])
  quant_calc=quant_calc[,st1:=quantile(mpd_space,0.75),by=.(state_cd,channel)]
  quant_calc=quant_calc[,n1:=quantile(mpd_space,0.25),by=.(channel)]
  quant_calc=quant_calc[,n2:=quantile(mpd_space,0.75),by=.(channel)]
  
  quant_calc=quant_calc[,ost1:=quantile(opp,0.85),by=.(state_cd,channel)]
  quant_calc=quant_calc[,ost2:=quantile(opp,0.90),by=.(state_cd,channel)]
  quant_calc=quant_calc[,on1:=quantile(opp,0.35),by=.(channel)]
  quant_calc=quant_calc[,on2:=quantile(opp,0.65),by=.(channel)]
  
  quant_calc=quant_calc[,allo:=2]
  quant_calc=quant_calc[mpd_space<n1,allo:=allo-1]
  quant_calc=quant_calc[mpd_space<n1 & mpd_space<st1,allo:=allo-1]
  quant_calc=quant_calc[mpd_space>n2 & mpd_space>st1,allo:=allo+1]
  #quant_calc=quant_calc[opp<on1 & opp<ost1,allo:=allo-1]
  quant_calc=quant_calc[opp>on2,allo:=allo+1]
  
  pbk_sp=merge(pbk_sp,quant_calc[,.(rtlr_num,allo)],by='rtlr_num')
  
  pbk_int1=pbk_int[is.na(SHLF_TOTAL) | exc==0]
  pbk_int1=pbk_int1[,opp:=sum(lift3*ptr_m)/sum(increment3),by=.(rtlr_num)]
  pbk_int1=pbk_int1[,tot_inc:=sum(increment3),by=.(rtlr_num)]
  pbk_int1=pbk_int1[tot_inc==0,opp:=0]
  
  quant_calc=unique(pbk_int1[,.(rtlr_num,cluster,state_cd,opp,channel)])
  quant_calc=quant_calc[,ost1:=quantile(opp,0.20),by=.(state_cd,channel)]
  quant_calc=quant_calc[,ost2:=quantile(opp,0.80),by=.(state_cd,channel)]
  quant_calc=quant_calc[,on1:=quantile(opp,0.25),by=.(channel)]
  quant_calc=quant_calc[,on2:=quantile(opp,0.90),by=.(channel)]
  
  quant_calc=quant_calc[,allo:=2]
  quant_calc=quant_calc[opp<on1,allo:=allo-1]
  quant_calc=quant_calc[opp<on1 & opp<ost1,allo:=allo-1]
  quant_calc=quant_calc[opp>on2 | opp>ost2,allo:=3]
  quant_calc=quant_calc[opp>on2 & opp>ost2,allo:=4]
  
  pbk_int1=merge(pbk_int1,quant_calc[,.(rtlr_num,allo)],by='rtlr_num')
  
  pbk_int=rbind(pbk_sp,pbk_int1,fill=T)
  
  pbk_int=pbk_int[,all_m:=median(allo,na.rm = T),by=.(state_cd,cluster)]
  pbk_int=pbk_int[is.na(allo),allo:=all_m]
  pbk_int=pbk_int[,all_m:=median(allo,na.rm = T)]
  pbk_int=pbk_int[is.na(allo),allo:=all_m]
  pbk_int=data.frame(pbk_int)[,c(names(pbk),'allo')]
  
  pbk=rbind(pbk_inc,pbk_int,fill=T)
  pbk=pbk[,allo1:=allo]
  pbk=pbk[is.na(allo),allo1:=0]
  pbk=pbk[,allo1:=max(allo1),by=.(rtlr_num)]
  pbk=pbk[,allo:=NULL]
  
  pbk=pbk[,store_total:=sum(current),by=.(rtlr_num)]
  pbk=pbk[,store_total_new:=sum(target3),by=.(rtlr_num)]
  
  pbk_sp=pbk[!is.na(SHLF_TOTAL) & exc>0]
  
  pbk_sp=pbk_sp[,mpd_space:=SHLF_TOTAL/store_total]
  pbk_sp=pbk_sp[,opp:=sum(lift3*ptr_m)/sum(increment3),by=.(rtlr_num)]
  pbk_sp=pbk_sp[,tot_inc:=sum(increment3),by=.(rtlr_num)]
  pbk_sp=pbk_sp[tot_inc==0,opp:=0]
  
  
  quant_calc=unique(pbk_sp[,.(rtlr_num,cluster,state_cd,mpd_space,opp,channel)])
  quant_calc=quant_calc[,st1:=quantile(mpd_space,0.25),by=.(state_cd,channel)]
  quant_calc=quant_calc[,st2:=quantile(mpd_space,0.8),by=.(state_cd,channel)]
  quant_calc=quant_calc[,n1:=quantile(mpd_space,0.25),by=.(channel)]
  quant_calc=quant_calc[,n2:=quantile(mpd_space,0.75),by=.(channel)]
  
  
  quant_calc=quant_calc[,ost1:=quantile(opp,0.05),by=.(state_cd,channel)]
  quant_calc=quant_calc[,ost2:=quantile(opp,0.95),by=.(state_cd,channel)]
  quant_calc=quant_calc[,on1:=quantile(opp,0.15),by=.(channel)]
  quant_calc=quant_calc[,on2:=quantile(opp,0.85),by=.(channel)]
  
  quant_calc=quant_calc[,allo:=4]
  quant_calc=quant_calc[mpd_space<n1,allo:=allo-2]
  quant_calc=quant_calc[mpd_space<n1 & mpd_space<st1,allo:=allo-1]
  quant_calc=quant_calc[mpd_space>st2,allo:=allo+1]
  quant_calc=quant_calc[mpd_space>n2,allo:=allo+2]
  #quant_calc=quant_calc[opp<on1 & opp<ost1,allo:=allo-1]
  quant_calc=quant_calc[opp>on2,allo:=allo+1]
  quant_calc=quant_calc[opp<on1,allo:=allo-1]
  quant_calc=quant_calc[opp>ost2,allo:=8]
  quant_calc=quant_calc[opp<ost1,allo:=0]
  quant_calc=quant_calc[allo<0,allo:=0]
  quant_calc=quant_calc[allo>8,allo:=8]
  
  pbk_sp=merge(pbk_sp,quant_calc[,.(rtlr_num,allo)],by='rtlr_num')
  
  pbk_int1=pbk[is.na(SHLF_TOTAL) | exc==0]
  pbk_int1=pbk_int1[,opp:=sum(lift3*ptr_m)/sum(increment3),by=.(rtlr_num)]
  pbk_int1=pbk_int1[,tot_inc:=sum(increment3),by=.(rtlr_num)]
  pbk_int1=pbk_int1[tot_inc==0,opp:=0]
  
  quant_calc=unique(pbk_int1[,.(rtlr_num,cluster,state_cd,opp,channel)])
  quant_calc=quant_calc[,ost1:=quantile(opp,0.20),by=.(state_cd,channel)]
  quant_calc=quant_calc[,ost2:=quantile(opp,0.80),by=.(state_cd,channel)]
  quant_calc=quant_calc[,on1:=quantile(opp,0.25),by=.(channel)]
  quant_calc=quant_calc[,on2:=quantile(opp,0.90),by=.(channel)]
  
  quant_calc=quant_calc[,allo:=4]
  quant_calc=quant_calc[opp<on1,allo:=allo-2]
  quant_calc=quant_calc[opp<on1 & opp<ost1,allo:=allo-2]
  quant_calc=quant_calc[opp>on2 | opp>ost2,allo:=6]
  quant_calc=quant_calc[opp>on2 & opp>ost2,allo:=8]
  
  pbk_int1=merge(pbk_int1,quant_calc[,.(rtlr_num,allo)],by='rtlr_num')
  
  pbk=rbind(pbk_sp,pbk_int1,fill=T)
  
  pbk=pbk[,inc_mpd:=inc_rev/increment3]
  pbk=pbk[increment3==0,inc_mpd:=0]
  
  order=pbk[increment3>0,.(score=median(revn)),by=.(lola_grp)]
  order=order[is.na(score),score:=0]
  order=order[order(-score)]
  
  aa=unique(pbk[,.(allo,dma_key,channel,rtlr_num)])
  pbk=merge(pbk,aa[,.(total_act=sum(allo)),by=.(dma_key,channel)],by=c('channel','dma_key'))
  pbk=pbk[,brnd_all0:=round(total_act*lift_share,0)]
  pbk=pbk[,inc4:=0]
  pbk[,introduction:=0]
  pbk[current==0,introduction:=1]
  pbk[,allo1:=allo1-1]
  pbk[allo1<0,allo1:=0]
  pbk[,allo:=allo+1]
  pbk[,allo_v:=allo]
  pbk[,allo1_v:=allo1]
  
  
  
  l_groups=order$lola_grp[order$score>0]
  for(i in 1:length(l_groups)){
    pbk[,ch:=0]
    pbk[lola_grp==l_groups[i],ch:=1]
    pbk=pbk[order(-ch,-inc_mpd)]
    pbk=pbk[current==0 & increment3>0 & ch==1 & allo1_v>0,inc4:=increment3]
    pbk=pbk[current>0 & increment3>0 & ch==1 & allo_v>0,inc4:=increment3]
    pbk=pbk[ch==1,done:=cumsum(inc4),by=.(channel,dma_key)]
    pbk=pbk[done-brnd_all0>0 & done-brnd_all0<3,inc4:=inc4-(done - brnd_all0)]
    pbk=pbk[done-brnd_all0>2,inc4:=0]
    pbk=pbk[inc4<0,inc4:=0]
    pbk=pbk[,allo1_v:=allo1 - sum(introduction*inc4),by=.(rtlr_num)]
    pbk=pbk[,allo_v:=allo - sum(inc4),by=.(rtlr_num)]  
  }
  
  
  return(pbk)
}

pbk_opt=const_opt(by=by, share=share, split=split)

pbk_opt[,lift3:=lift3*inc4/increment3]
pbk_opt[increment3==0,lift3:=0]
pbk_opt[,increment3:=inc4]
pbk_opt[,target3:=current + increment3]
pbk_opt=pbk_opt[,inc_rev:=lift3*ptr_m]
pbk_opt=pbk_opt[increment3>0,inc_mpd:=inc_rev/increment3]
pbk_opt=pbk_opt[increment3==0,inc_mpd:=0]



names(pbk_opt)
pbk_raw=pbk_opt[,.(rtlr_num,channel,wslr_nbr,cluster,dma_key,state_cd,licence,lola_grp,current,lift,increment,target,lift3,increment3,target3,segment)]

names(pbk_raw)[c(7:15)]=c('account_3.2','brand','POD','unconstrained_oppty','unconstrained_increment','unconstrained_target','store_initial_oppty','store_increment','store_initial_target')

fwrite(pbk_raw,row.names = F,'volume_model_offpremise_constrained_dec_raw.csv',sep=',')

pbk_opt=pbk_opt[current!=0 | increment3>0]
pbk_opt=pbk_opt[order(rtlr_num,-inc_mpd, -current)]

pbk_opt=pbk_opt[,numb:=1]
pbk_opt=pbk_opt[,numb:=cumsum(numb),by=.(rtlr_num)]
pbk_opt=pbk_opt[numb<16]

pbk_raw=pbk_opt[,.(rtlr_num,channel,wslr_nbr,cluster,dma_key,state_cd,licence,lola_grp,current,lift,increment,target,lift3,increment3,target3)]

names(pbk_raw)[c(7:15)]=c('account_3.2','brand','POD','unconstrained_oppty','unconstrained_increment','unconstrained_target','store_initial_oppty','store_increment','store_initial_target')

fwrite(pbk_raw,row.names = F,'volume_model_offpremise_constrained_1223.csv',sep=',')


##############on premise###############

pbk1=fread('GBM_correctedresONP_2018-12-23.csv')
pbk1$channel='resONP'
pbk1$X=NULL
pbk2=fread('GBM_correctedbarONP_2018-12-23.csv')
pbk2$channel='barONP'
pbk2$X=NULL
mpd=fread('mpd_month_onpremise_2.csv')

pbk=rbind(pbk1,pbk2)

pbk=data.table(pbk)
mpd=data.table(mpd)


pbk=pbk[,draft_pack:=substr(lola_grp,0,1)]
pbk=pbk[,temp:=nchar(as.character(lola_grp))]
pbk=pbk[,brand:=substr(lola_grp,3,temp),by=.(temp)]

mpd=mpd[,draft_pack:=substr(lola_grp,0,1)]

mpd=mpd[cal_yr_mo_nbr>201802,.(mpd=sum(mpd)),by=.(rtlr_num,draft_pack)]

pbk=merge(pbk,mpd,by=c('rtlr_num','draft_pack'),all.x=T)
pbk=pbk[is.na(mpd),mpd:=0]

pbk[mpd==0,increment:=0]
pbk[mpd==0,target:=current]
pbk[mpd==0,lift:=0]

brnd_allowed$lola_grp=tolower(as.character(brnd_allowed$lola_grp))
pbk=merge(pbk,license,by='rtlr_num',all.x = T)
pbk=pbk[is.na(licence),licence:=0]
names(brnd_allowed)[1]='brand'
pbk=merge(pbk,brnd_allowed,by='brand',all.x = T)
pbk=pbk[is.na(cnt),cnt:=0]

pbk[cnt==0 & licence==1,increment:=0]
pbk[cnt==0 & licence==1,target:=current]
pbk[cnt==0 & licence==1,lift:=0]


names(brnd_carried)[2]='brand'
pbk=merge(pbk,rtlr_map,by='rtlr_num')
pbk=merge(pbk,brnd_carried,by=c('wslr_nbr','brand'),all.x = T)
pbk[is.na(PKG_COUNT),PKG_COUNT:=0]
pbk=pbk[,target:=target/4]
pbk=pbk[,current:=current/4]

pbk=pbk[,target2:=target]
pbk[PKG_COUNT<target,target2:=PKG_COUNT]
pbk[PKG_COUNT<target & PKG_COUNT < current,target2:=current]

pbk=pbk[,lift2:=lift]
pbk=pbk[target>current,lift2:=lift*(target2-current)/(target-current)]

pbk=pbk[,increment2:=target2-current]

pbk=pbk[,inc_rev:=ptr_m*lift2]
pbk=pbk[,inc_mpd:=inc_rev/increment2]


pbk=pbk[order(rtlr_num,-inc_mpd)]

pbk=pbk[,numb:=1]
pbk=pbk[,numb:=cumsum(numb),by=.(rtlr_num)]
pbk=pbk[numb<16]


names(pbk)
pbk=pbk[,.(rtlr_num,channel,wslr_nbr,cluster,dma_key,state_cd,licence,brand,draft_pack,current,lift,increment,target,lift2,increment2,target2)]

names(pbk)[c(7:16)]=c('account_3.2','brand','pack_draft','POD','unconstrained_oppty','unconstrained_increment','unconstrained_target','store_initial_oppty','store_increment','store_initial_target')

pbk_off=fread('volume_model_offpremise_constrained_1223.csv')

pbk=rbind(pbk,pbk_off,fill=T)

pbk$model='Base_co_maco'
fwrite(pbk,row.names = F,'volume_model_constrained_1223.csv',sep = ',')
