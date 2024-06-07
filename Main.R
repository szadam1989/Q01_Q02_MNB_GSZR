library("RODBC")
channelOracle <- odbcDriverConnect(paste("DRIVER={Oracle in OraClient18Home1};DBQ=EMERALD.KSH.HU;UID=", Sys.getenv("userid"), ";PWD=", Sys.getenv("pwd")), DBMSencoding = "latin1")

(LAST_RUNNING <- sqlQuery(channelOracle, "select PARAM_ERTEK from VB_REP.VB_APP_INIT where ALKALMAZAS = 'MNB napi változáslista küldése' and PROGRAM = 'mnb_EBEAD.sql' and PARAM_NEV = 'utolso_futas'"))
#2024-06-06 10:30:01
#str(LAST_RUNNING)
#LAST_RUNNING[1] <- "2024-01-11 10:30:02"
#CHANGED_ON_2023061718 <- sqlQuery(channelOracle, "select * from VB_REP.MNB_NAPI")
CHANGED_ON_20240607 <- sqlQuery(channelOracle, "select * from VB_REP.MNB_NAPI")
View(CHANGED_ON_20240607[CHANGED_ON_20240607$KOD == "Q01",])
View(CHANGED_ON_20240607[CHANGED_ON_20240607$KOD == "Q02",])
View(CHANGED_ON_20240607[CHANGED_ON_20240607$KOD == "Q03",])


#write.table(Q01_MENT$X4, "filename.txt", sep="\n", row.names=FALSE, col.names = FALSE, quote = FALSE)
HIST_ALAKDAT <- sqlQuery(channelOracle, paste("select M003, datum from VB.F003_HIST3PR where alakdat != alakdat_u order by DATUM desc"))
View(HIST_ALAKDAT)
HIST_ALAKDAT_SZUKEBB <- sqlQuery(channelOracle, paste("select M003, datum from VB.F003_HIST3PR where alakdat != alakdat_u and M003 in (select M003 from VB.F003 where substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811') order by DATUM desc"))
View(HIST_ALAKDAT_SZUKEBB)
HIST_UELESZT <- sqlQuery(channelOracle, paste("select M003, UELESZT, UELESZT_R from VB.F003 where UELESZT is not null and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811' order by UELESZT_R desc"))
View(HIST_UELESZT)
#HIST_LETSZAM_H <- sqlQuery(channelOracle, paste("select M003, LETSZAM_H, LETSZAM_R from VB.F003 where LETSZAM_H >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811' order by LETSZAM_H desc"))
#View(HIST_LETSZAM_H)
#HIST_LETSZAM_R <- sqlQuery(channelOracle, paste("select M003, LETSZAM_H, LETSZAM_R from VB.F003 where LETSZAM_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811' order by LETSZAM_R desc"))
#View(HIST_LETSZAM_R)
#library(openxlsx)
#write.xlsx(HIST_UELESZT, 'UELESZT.xlsx', sheetName = 'UELESZT', append = TRUE, row.names = FALSE)



#Q01
NEW_M003 <- sqlQuery(channelOracle, paste("select M003, M003_R from VB.F003 where M003_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_M0491_R <- sqlQuery(channelOracle, paste("select M003, M0491_R from VB.F003 where M0491_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and M0491_F != '06' and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_M005_SZH_R <- sqlQuery(channelOracle, paste("select M003, M005_SZH_R from VB.F003 where M005_SZH_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_NEV_R <- sqlQuery(channelOracle, paste("select M003, NEV_R from VB.F003 where NEV_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_RNEV_R <- sqlQuery(channelOracle, paste("select M003, RNEV_R from VB.F003 where RNEV_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_SZEKHELY_R <- sqlQuery(channelOracle, paste("select M003, SZEKHELY_R from VB.F003 where SZEKHELY_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_LEVELEZESI_R <- sqlQuery(channelOracle, paste("select M003, LEVELEZESI_R from VB.F003 where LEVELEZESI_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_LEV_PF_R <- sqlQuery(channelOracle, paste("select M003, LEV_PF_R from VB.F003 where LEV_PF_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_M040K_R <- sqlQuery(channelOracle, paste("select M003, M040K_R from VB.F003 where M040K_R > TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_M040V_R <- sqlQuery(channelOracle, paste("select M003, M040V_R from VB.F003 where M040V_R > TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_LETSZAM_R <- sqlQuery(channelOracle, paste("select M003, LETSZAM_R from VB.F003 where LETSZAM_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_ARBEV_R <- sqlQuery(channelOracle, paste("select M003, ARBEV_R from VB.F003 where ARBEV_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_M0781_R <- sqlQuery(channelOracle, paste("select M003, M0781_R from VB.F003 where M0781_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_M0581_R <- sqlQuery(channelOracle, paste("select M003, M0581_R from VB.F003 where M0581_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
#CHANGED_M063_R <- sqlQuery(channelOracle, paste("select M003, M063_R from VB.F003 where M063_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
#CHANGED_MP65_R <- sqlQuery(channelOracle, paste("select M003, MP65_R from VB.F003 where MP65_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_UELESZT_R <- sqlQuery(channelOracle, paste("select M003, UELESZT_R from VB.F003 where UELESZT_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_CEGV_R <- sqlQuery(channelOracle, paste("select M003, CEGV_R from VB.F003 where CEGV_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_MVB39_R <- sqlQuery(channelOracle, paste("select M003, MVB39_R from VB.F003 where MVB39_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_M0582_R <- sqlQuery(channelOracle, paste("select * from VB.F003_M0582 where M0582_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')"))

HATALYVEGE <- sqlQuery(channelOracle, paste("select M003, M0582, TO_CHAR(M0582_HV, 'YYYYMMDD') M0582_HV from VB.F003_M0582 where M003 not in (select M003 from VB.F003_M0582 where M0582_HV is null) order by M003"))
HATALYOS <- sqlQuery(channelOracle, paste("select M003, M0582, TO_CHAR(M0582_H, 'YYYYMMDD') M0582_H from VB.F003_M0582 where M0582_HV is null order by M003"))

CHANGED_HIST_ALAKDAT <- sqlQuery(channelOracle, paste("select M003, datum, alakdat_u from VB.F003_HIST3PR where datum >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and alakdat != alakdat_u"))

PLUS_M003 <- sqlQuery(channelOracle, paste("select M003, param_dtol from VB_REP.VB_APP_INIT where program = 'mnb_EBEAD.sql' and TO_DATE(param_dtol, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and param_nev = 'm003'"))
MINUS_M003 <- sqlQuery(channelOracle, paste("select M003, param_dtol from VB_REP.VB_APP_INIT where program = 'mnb_EBEAD.sql' and TO_DATE(param_dtol, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and param_nev = '-m003'"))
#CHANGED_CEGV_R[CHANGED_CEGV_R$M003 %in% NEW_M003$M003, ]
#CHANGED_ARBEV_R[CHANGED_ARBEV_R$M003 %in% NEW_M003$M003, ]
View(CHANGED_M0491_R[CHANGED_M0491_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_M005_SZH_R[CHANGED_M005_SZH_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_NEV_R[CHANGED_NEV_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_RNEV_R[CHANGED_RNEV_R$M003 %in% NEW_M003$M003, ])#Külön kell megszámolni az érintett új törzsszámokat
View(CHANGED_SZEKHELY_R[CHANGED_SZEKHELY_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_LEVELEZESI_R[CHANGED_LEVELEZESI_R$M003 %in% NEW_M003$M003, ])#Külön kell megszámolni az érintett új törzsszámokat
View(CHANGED_LEV_PF_R[CHANGED_LEV_PF_R$M003 %in% NEW_M003$M003, ])#Külön kell megszámolni az érintett új törzsszámokat
View(CHANGED_M040K_R[CHANGED_M040K_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_M040V_R[CHANGED_M040V_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_M0781_R[CHANGED_M0781_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_LETSZAM_R[CHANGED_LETSZAM_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_ARBEV_R[CHANGED_ARBEV_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_M0581_R[CHANGED_M0581_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_M063_R[CHANGED_M063_R$M003 %in% NEW_M003$M003, ])#Külön kell megszámolni az érintett új törzsszámokat
View(CHANGED_UELESZT_R[CHANGED_UELESZT_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_CEGV_R[CHANGED_CEGV_R$M003 %in% NEW_M003$M003, ])#Külön kell megszámolni az érintett új törzsszámokat
View(CHANGED_MVB39_R[CHANGED_MVB39_R$M003 %in% NEW_M003$M003, ])
View(CHANGED_M040V_R[CHANGED_M040V_R$M003 %in% CHANGED_M040K_R$M003, ])
CHANGED_M040V_R[CHANGED_M040V_R$M003 %in% NEW_M003$M003, ]
CHANGED_UELESZT_R[CHANGED_UELESZT_R$M003 %in% CHANGED_M040K_R$M003, ]
#CHANGED_UELESZT_R[CHANGED_UELESZT_R$M003 %in% CHANGED_ARBEV_R$M003, ]
View(CHANGED_RNEV_R[CHANGED_RNEV_R$M003 %in% CHANGED_NEV_R$M003, ])
View(CHANGED_RNEV_R[CHANGED_RNEV_R$M003 %in% CHANGED_CEGV_R$M003, ])
View(CHANGED_CEGV_R[CHANGED_CEGV_R$M003 %in% CHANGED_RNEV_R$M003, ])
View(CHANGED_M005_SZH_R[CHANGED_M005_SZH_R$M003 %in% CHANGED_SZEKHELY_R$M003, ])
View(CHANGED_LETSZAM_R[CHANGED_LETSZAM_R$M003 %in% CHANGED_CEGV_R$M003, ])
View(CHANGED_LETSZAM_R[CHANGED_LETSZAM_R$M003 %in% CHANGED_M0781_R$M003, ])
View(CHANGED_LETSZAM_R[CHANGED_LETSZAM_R$M003 %in% CHANGED_ARBEV_R$M003, ])

View(CHANGED_ARBEV_R)
View(CHANGED_LETSZAM_R)
View(CHANGED_M0581_R)

View(CHANGED_M040K_R)
View(CHANGED_M040V_R)
View(CHANGED_M0581_R)
#CHANGED_M0581_R <- CHANGED_M0581_R[CHANGED_M0581_R$M003 != "27286438", ]

#CHANGED_M040K_R <- CHANGED_M040K_R[CHANGED_M040K_R$M003 != "13873442", ]
#CHANGED_M040V_R <- CHANGED_M040V_R[CHANGED_M040V_R$M003 != "27456961", ]
View(NEW_M003[NEW_M003$M003 == "18081307", ])
View(CHANGED_M0491_R[CHANGED_M0491_R$M003 == "18081307", ])#Benne van
View(CHANGED_M005_SZH_R[CHANGED_M005_SZH_R$M003 == "18081307", ])
View(CHANGED_M040K_R[CHANGED_M040K_R$M003 == "18081307", ])



View(NEW_M003[NEW_M003$M003 == "26371098", ])
View(CHANGED_M0491_R[CHANGED_M0491_R$M003 == "26371098", ])#Benne van
View(CHANGED_M005_SZH_R[CHANGED_M005_SZH_R$M003 == "26371098", ])
View(CHANGED_M040K_R[CHANGED_M040K_R$M003 == "26371098", ])
View(CHANGED_M0582_R[CHANGED_M0582_R$M003 == "26371098", ])

#CHANGED_M040K_R2 <- sqlQuery(channelOracle, paste("select M003, M040K_R from VB.F003 where M040K_R > TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))

#View(CHANGED_M040K_R[CHANGED_M040K_R$M003 == "11941396", ])
#CHANGED_M040K_R2 <- CHANGED_M040K_R2[CHANGED_M040K_R2$M003 != "29088449" & CHANGED_M040K_R2$M003 != "26367349" & CHANGED_M040K_R2$M003 != "11033781" & CHANGED_M040K_R2$M003 != "27109236", ]


#View(CHANGED_ALL_M003[CHANGED_ALL_M003 %in% CHANGED_LETSZAM_R$M003])

#View(CHANGED_Q02_1)
#CHANGED_Q02_1 <- CHANGED_Q02_1[CHANGED_Q02_1$M003_JE != "18417384", ]

#Q02
CHANGED_Q02_1 <- sqlQuery(channelOracle, paste("select M003_JE, M003_JU from VB.F003_JUJE JUJE, VB.F003 FO where JUJE.M003_JE = FO.M003 and (JUJE.JEJU_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') or JUJE.DATUM_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')) and substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811'"))
CHANGED_Q02_2 <- sqlQuery(channelOracle, paste("select M003_JE, M003_JU from VB.F003_JUJE JUJE, (select M003, PARAM_DTOL from VB_REP.VB_APP_INIT where M003 is not null) c where JUJE.M003_JE = c.M003 and TO_DATE(PARAM_DTOL, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')"))
CHANGED_Q02_3 <- sqlQuery(channelOracle, paste("select M003_JE, M003_JU from VB_CEG.JOGUTOD j, VB.F003 g where kulf_ju = '1' and g.M003 = M003_JE and g.M040K_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and g.M040 = '9'"))
CHANGED_Q02_4 <- sqlQuery(channelOracle, paste("select distinct M003_JE, M003_JU from VB_CEG.JOGUTOD j, VB_REP.VB_APP_INIT g, vb.f003 r where KULF_JU = '1' and g.M003 = M003_JE and g.M003 = r.M003 and datum_r = (select max(datum_r) from VB_CEG.JOGUTOD where M003_JE = j.M003_JE) and TO_DATE(param_dtol, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')"))


odbcClose(channelOracle)


#Q01
NEW_M003[, "M003"] <- sapply(NEW_M003[, "M003"], as.character)
CHANGED_M005_SZH_R[, "M003"] <- sapply(CHANGED_M005_SZH_R[, "M003"], as.character)
CHANGED_NEV_R[, "M003"] <- sapply(CHANGED_NEV_R[, "M003"], as.character)
CHANGED_RNEV_R[, "M003"] <- sapply(CHANGED_RNEV_R[, "M003"], as.character)
CHANGED_SZEKHELY_R[, "M003"] <- sapply(CHANGED_SZEKHELY_R[, "M003"], as.character)
CHANGED_LEVELEZESI_R[, "M003"] <- sapply(CHANGED_LEVELEZESI_R[, "M003"], as.character)
CHANGED_LEV_PF_R[, "M003"] <- sapply(CHANGED_LEV_PF_R[, "M003"], as.character)
CHANGED_M040K_R[, "M003"] <- sapply(CHANGED_M040K_R[, "M003"], as.character)
CHANGED_M040V_R[, "M003"] <- sapply(CHANGED_M040V_R[, "M003"], as.character)
CHANGED_LETSZAM_R[, "M003"] <- sapply(CHANGED_LETSZAM_R[, "M003"], as.character)
CHANGED_ARBEV_R[, "M003"] <- sapply(CHANGED_ARBEV_R[, "M003"], as.character)
CHANGED_M0781_R[, "M003"] <- sapply(CHANGED_M0781_R[, "M003"], as.character)
CHANGED_M0581_R[, "M003"] <- sapply(CHANGED_M0581_R[, "M003"], as.character)
#CHANGED_M063_R[, "M003"] <- sapply(CHANGED_M063_R[, "M003"], as.character)
#CHANGED_MP65_R[, "M003"] <- sapply(CHANGED_MP65_R[, "M003"], as.character)
CHANGED_UELESZT_R[, "M003"] <- sapply(CHANGED_UELESZT_R[, "M003"], as.character)
CHANGED_CEGV_R[, "M003"] <- sapply(CHANGED_CEGV_R[, "M003"], as.character)
CHANGED_MVB39_R[, "M003"] <- sapply(CHANGED_MVB39_R[, "M003"], as.character)
CHANGED_M0491_R[, "M003"] <- sapply(CHANGED_M0491_R[, "M003"], as.character)
CHANGED_M0582_R[, "M003"] <- sapply(CHANGED_M0582_R[, "M003"], as.character)
HATALYOS[, "M003"] <- sapply(HATALYOS[, "M003"], as.character)
HATALYVEGE[, "M003"] <- sapply(HATALYVEGE[, "M003"], as.character)
CHANGED_HIST_ALAKDAT[, "M003"] <- sapply(CHANGED_HIST_ALAKDAT[, "M003"], as.character)
PLUS_M003[, "M003"] <- sapply(PLUS_M003[, "M003"], as.character)

CHANGED_ALL_M003 <- c(NEW_M003$M003, CHANGED_M005_SZH_R$M003, CHANGED_NEV_R$M003, CHANGED_RNEV_R$M003, CHANGED_SZEKHELY_R$M003, CHANGED_LEVELEZESI_R$M003, CHANGED_LEV_PF_R$M003, CHANGED_M040K_R$M003, CHANGED_M040V_R$M003, CHANGED_LETSZAM_R$M003, CHANGED_ARBEV_R$M003, CHANGED_M0781_R$M003, CHANGED_M0581_R$M003, CHANGED_UELESZT_R$M003, CHANGED_CEGV_R$M003, CHANGED_MVB39_R$M003, CHANGED_M0491_R$M003, CHANGED_HIST_ALAKDAT$M003) #CHANGED_M063_R$M003 #CHANGED_M0582_R$M003,, PLUS_M003$M003
CHANGED_ALL_M003 <- unique(CHANGED_ALL_M003)
#CHANGED_MP65_R$M003,

library("RODBC")
channelOracle <- odbcDriverConnect(paste("DRIVER={Oracle in OraClient18Home1};DBQ=EMERALD.KSH.HU;UID=", Sys.getenv("userid"), ";PWD=", Sys.getenv("pwd")))


MennyiEzer <- as.integer(length(CHANGED_ALL_M003) / 1000)
tartomanyKezdet <- 1
if(MennyiEzer == 0){
  
  tartomanyVeg <- length(CHANGED_ALL_M003)
  
}else{
  
  tartomanyVeg <- 1000
  
}

#MennyiEzer + 1
for(tartomany in 0:MennyiEzer+1){
  print(tartomany)
  M003ToFind <- paste0("'", CHANGED_ALL_M003[c(tartomanyKezdet:tartomanyVeg)], "'", collapse=", ")
  whereIn <- paste0("(", M003ToFind, ")")
  #and M040K < TO_DATE('", Sys.Date() - 365, "', 'YYYY/MM/DD HH:MI:SS') 
  CHANGED_M040K <- sqlQuery(channelOracle, paste("select M003, M040K from VB.F003 where M040 in ('0','9') and TO_CHAR(M040K, 'YYYY') <> '", substr(Sys.Date(), 1, 4), "' and M003 in ", whereIn, " and M040K_R < TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')" , sep = ""))
  CHANGED_ALL_M003 <- CHANGED_ALL_M003[!(CHANGED_ALL_M003 %in% CHANGED_M040K$M003)]
  print(CHANGED_M040K)
  
  tartomanyKezdet <- tartomanyKezdet + 1000
  
  if(tartomany < MennyiEzer){
    
    tartomanyVeg <- tartomanyVeg + 1000
    
  }else{
    
    tartomanyVeg <- length(CHANGED_ALL_M003)
    
  }
  #break
  
}

odbcClose(channelOracle)


str(CHANGED_ALL_M003)
CHANGED_ALL_M003 <- CHANGED_ALL_M003[order(CHANGED_ALL_M003)]
View(CHANGED_ALL_M003)
length(CHANGED_ALL_M003)

CHANGED_ALL_M003 <- c(CHANGED_ALL_M003, PLUS_M003$M003, CHANGED_M0582_R$M003)
length(CHANGED_ALL_M003)
CHANGED_ALL_M003 <- unique(CHANGED_ALL_M003)
length(CHANGED_ALL_M003)
CHANGED_ALL_M003 <- CHANGED_ALL_M003[order(CHANGED_ALL_M003)]
#      M003      M040K 2023.05.25.
#      19271635 2006-04-03
#library(openxlsx)
#CHANGED_ALL_M003_DATA_FRAME <- as.data.frame(CHANGED_ALL_M003)
#write.xlsx(CHANGED_ALL_M003_DATA_FRAME, 'CHANGED_ALL_M003.xlsx', sheetName = 'CHANGED_ALL_M003', append = TRUE, row.names = FALSE)
#CHANGED_M0582_R_DATA_FRAME <- as.data.frame(CHANGED_M0582_R)
#write.xlsx(CHANGED_M0582_R_DATA_FRAME, 'CHANGED_M0582_R.xlsx', sheetName = 'CHANGED_M0582_R', append = TRUE, row.names = FALSE)





#CHANGED_ALL_M003_WITHOUT_UELESZT <- c(NEW_M003$M003, CHANGED_M005_SZH_R$M003, CHANGED_NEV_R$M003, CHANGED_RNEV_R$M003, CHANGED_SZEKHELY_R$M003, CHANGED_LEVELEZESI_R$M003, CHANGED_LEV_PF_R$M003, CHANGED_M040K_R$M003, CHANGED_M040V_R$M003, CHANGED_LETSZAM_R$M003, CHANGED_ARBEV_R$M003, CHANGED_M0781_R$M003, CHANGED_M0581_R$M003, CHANGED_M063_R$M003, CHANGED_CEGV_R$M003, CHANGED_MVB39_R$M003, CHANGED_M0491_R$M003, CHANGED_HIST_ALAKDAT$M003) #CHANGED_M0582_R$M003,, PLUS_M003$M003
#CHANGED_ALL_M003_WITHOUT_UELESZT <- unique(CHANGED_ALL_M003_WITHOUT_UELESZT)
#length(CHANGED_ALL_M003_WITHOUT_UELESZT)



#NEW_M003[NEW_M003$M003 == "19303758" | NEW_M003$M003 == "18599433" | NEW_M003$M003 == "18113554", ]
#CHANGED_M0491_R[CHANGED_M0491_R$M003 == "19303758" | CHANGED_M0491_R$M003 == "18599433" | CHANGED_M0491_R$M003 == "18113554", ]
#CHANGED_M040K_R[CHANGED_M040K_R$M003 == "19303758" | CHANGED_M040K_R$M003 == "18599433" | CHANGED_M040K_R$M003 == "18113554", ]
#CHANGED_CEGV_R[CHANGED_CEGV_R$M003 == "19303758" | CHANGED_CEGV_R$M003 == "18599433" | CHANGED_CEGV_R$M003 == "18113554", ]


#Q02
CHANGED_Q02_1[, "M003_JE"] <- sapply(CHANGED_Q02_1[, "M003_JE"], as.character)
CHANGED_Q02_2[, "M003_JE"] <- sapply(CHANGED_Q02_2[, "M003_JE"], as.character)
CHANGED_Q02_3[, "M003_JE"] <- sapply(CHANGED_Q02_3[, "M003_JE"], as.character)
CHANGED_Q02_4[, "M003_JE"] <- sapply(CHANGED_Q02_4[, "M003_JE"], as.character)

CHANGED_Q02_1[, "M003_JU"] <- sapply(CHANGED_Q02_1[, "M003_JU"], as.character)
CHANGED_Q02_2[, "M003_JU"] <- sapply(CHANGED_Q02_2[, "M003_JU"], as.character)
CHANGED_Q02_3[, "M003_JU"] <- sapply(CHANGED_Q02_3[, "M003_JU"], as.character)
CHANGED_Q02_4[, "M003_JU"] <- sapply(CHANGED_Q02_4[, "M003_JU"], as.character)


CHANGED_ALL_M003_JE_JU <- rbind(CHANGED_Q02_1, CHANGED_Q02_2, CHANGED_Q02_3, CHANGED_Q02_4)
CHANGED_ALL_M003_JE_JU <- unique(CHANGED_ALL_M003_JE_JU)
CHANGED_ALL_M003_JE_JU <- CHANGED_ALL_M003_JE_JU[order(CHANGED_ALL_M003_JE_JU$M003_JE, CHANGED_ALL_M003_JE_JU$M003_JU), ]
dim(CHANGED_ALL_M003_JE_JU)
str(CHANGED_ALL_M003_JE_JU)
View(CHANGED_ALL_M003_JE_JU)

for(i in 1:nrow(CHANGED_ALL_M003_JE_JU)){
  
  if(CHANGED_ALL_M003_JE_JU[i, "M003_JE"] == CHANGED_ALL_M003_JE_JU[i, "M003_JU"]){
    
    print("A jogelőd és a jogutód megegyezik.")
    
  }
  
}