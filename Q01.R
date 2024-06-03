Q01 <- data.frame(matrix(NA, nrow = length(CHANGED_ALL_M003), ncol = 4))
#substr(Sys.Date(), 9, 10)
for(row in 1:length(CHANGED_ALL_M003)){
  
  Q01[row, 1] <- "Q01"
  
  Q01[row, 2] <- paste("Q01", substr(Sys.Date(), 4, 4), substr(Sys.Date(), 6, 7), substr(Sys.Date(), 9, 10), "15302724", sep = "")
  
  Q01[row, 3] <- row
  
}
View(Q01)


library("RODBC")
channelOracle <- odbcDriverConnect(paste("DRIVER={Oracle in OraClient18Home1};DBQ=EMERALD.KSH.HU;UID=", Sys.getenv("userid"), ";PWD=", Sys.getenv("pwd")))
datum <- paste(substr(Sys.Date(), 1, 4), substr(Sys.Date(), 6, 7), substr(Sys.Date(), 9, 10), sep = "")

#
for(row in 1:length(CHANGED_ALL_M003)){
  
  if(nchar(row) == 1){
    
    KSHTORZS <- paste("000000", row, sep = "")
    
  }else if(nchar(row) == 2){
    
    KSHTORZS <- paste("00000", row, sep = "")
    
  }else if(nchar(row) == 3){
    
    KSHTORZS <- paste("0000", row, sep = "")
    
  }else if(nchar(row) == 4){
    
    KSHTORZS <- paste("000", row, sep = "")
    
  }else if(nchar(row) == 5){
    
    KSHTORZS <- paste("00", row, sep = "")
    
  }
  
  VALUES <- sqlQuery(channelOracle, paste("select M003, M0491, TO_CHAR(M0491_H, 'YYYYMMDD') M0491_H, M005_SZH, TO_CHAR(M005_SZH_H, 'YYYYMMDD') M005_SZH_H, nev, TO_CHAR(nev_h, 'YYYYMMDD') nev_h, rnev, TO_CHAR(rnev_h, 'YYYYMMDD') RNEV_H, M054_SZH, TELNEV_SZH, UTCA_SZH, TO_CHAR(szekhely_h, 'YYYYMMDD') SZEKHELY_H, M054_LEV, telnev_lev, UTCA_LEV, TO_CHAR(levelezesi_r, 'YYYYMMDD') LEVELEZESI_R, M054_PF_LEV, telnev_PF_LEV, PFIOK_LEV, to_char(lev_pf_r, 'YYYYMMDD'), M040, to_char(M040K, 'YYYYMMDD'), M025, to_char(letszam_h, 'YYYYMMDD'), M026, to_char(arbev_h, 'YYYYMMDD') ARBEV_H, M009_SZH, to_char(alakdat, 'YYYYMMDD') ALAKDAT, M0781, to_char(M0781_H, 'YYYYMMDD'), M058_J, to_char(M0581_H, 'YYYYMMDD'), decode(MP65, 'S9900', null, MP65) MP65, to_char(MP65_H,'YYYYMMDD') MP65_H, to_char(UELESZT, 'YYYYMMDD') UELESZT, to_char(M003_R, 'YYYYMMDD') M003_R, to_char(DATUM, 'YYYYMMDD') DATUM, M0581, to_char(M0581_H, 'YYYYMMDD') M0581_H, cegv, to_char(cegv_h, 'YYYYMMDD') CEGV_H, nvl(MVB39, '0') MVB39, nvl(to_char(MVB39_H, 'YYYYMMDD'), case when to_char(alakdat, 'YYYY') < '2016' then '20160101' else to_char(alakdat, 'YYYYMMDD') end) MVB39_H, null ORSZ, LETSZAM, ARBEV from VB.F003 where M003 = '", CHANGED_ALL_M003[row], "'"))
  VALUES[, 1:ncol(VALUES)] <- sapply(VALUES[, 1:ncol(VALUES)], as.character)
  VALUES[is.na(VALUES)] <- ""
  #to_char(mukodv, 'YYYYMMDD'), 
  
  if(nchar(VALUES[1, "M005_SZH"]) == 1){
    
    M005_SZH <- paste("0", VALUES[1, "M005_SZH"], sep = "")
    
  }else{
    
    M005_SZH <- VALUES[1, "M005_SZH"]
    
  } 
  
  
  if(nchar(VALUES[1, "M025"]) == 1){
    
    M025 <- paste("0", VALUES[1, "M025"], sep = "")
    
  }else{
    
    M025 <- VALUES[1, "M025"]
    
  }
  
  
  if(nchar(VALUES[1, "M0781"]) == 3){
    
    M0781 <- paste("0", VALUES[1, "M0781"], sep = "")
    
  }else{
    
    M0781 <- VALUES[1, "M0781"]
    
  }
  
  
  if(nchar(VALUES[1, "M058_J"]) == 3){
    
    M058_J <- paste("0", VALUES[1, "M058_J"], sep = "")
    
  }else{
    
    M058_J <- VALUES[1, "M058_J"]
    
  }
  

  if(VALUES[1, "MP65"] == ""){
    
    MP65_H <- NULL
    
  }else{
    
    MP65_H <- VALUES[1, "MP65_H"]
    
  }
    
  PFIOK_LEV <- VALUES[1, "PFIOK_LEV"]
  
  
  if(nchar(VALUES[1, "CEGV"]) == 9){
    
    CEGV <- paste("0", VALUES[1, "CEGV"], sep = "")
    
  }else{
    
    CEGV <- VALUES[1, "CEGV"]
  }
  
  
  if(nchar(VALUES[1, "ARBEV_H"]) == 0){
    
    ARBEV_H <- VALUES[1, "ALAKDAT"]
    
  }else{
    
    ARBEV_H <- VALUES[1, "ARBEV_H"]
    
  }
  
  
  if(nchar(VALUES[1, "M009_SZH"]) == 3){
    
    M009_SZH <- paste("0", VALUES[1, "M009_SZH"], sep = "")
    
  }else if(nchar(VALUES[1, "M009_SZH"]) == 2){
    
    M009_SZH <- paste("00", VALUES[1, "M009_SZH"], sep = "")
    
  }else{
    
    M009_SZH <- VALUES[1, "M009_SZH"]
    
  }
  
  M009_SZH_CDV <- sqlQuery(channelOracle, paste("select M009CDV from VT.F009_AKT where M009 = '", M009_SZH, "'", sep = ""))
  M009_SZH_CDV_TOGETHER <- paste(M009_SZH, M009_SZH_CDV, sep = "")
  
  ORSZ <- sqlQuery(channelOracle, paste("select ORSZ from (select * from VB_CEG.VB_APEH_CIM where M003 = '", CHANGED_ALL_M003[row], "'  order by DATUM_R desc) where rownum < 2", sep = ""))
  
  if(VALUES[1, "ORSZ"] == "HU" & VALUES[1, "M005_SZH"] > 20){ 
    
    ORSZ <- "Z8"
    cat(paste("A", VALUES[1, "M003"], "törzsszám országkódja HU-ról Z8 lett, mert a megyekód (M005_SZH = ", VALUES[1, "M005_SZH"], ") > 20).", sep = " "))
    
  }
  
  if(VALUES[1, "ORSZ"] == "XX" ){ 
    
    ORSZ <- "Z8"
    cat(paste("A", VALUES[1, "M003"], "törzsszám országkódja XX-ről Z8 lett, mert XX értéket az MNB nem tud fogadni.", sep = " "))
    
  }
  
  
  if(nrow(ORSZ) == 0 || is.na(ORSZ) == TRUE){
    
    if(VALUES[1, "ORSZ"] == "" & as.numeric(VALUES[1, "M005_SZH"]) < 21){ 
      
      ORSZ <- "HU"
      cat(paste("A", VALUES[1, "M003"], "törzsszám országkódja üresről HU lett, mert a megyekód (M005_SZH = ", VALUES[1, "M005_SZH"], ") < 21.", sep = " "))
      
    }
    
    if(VALUES[1, "ORSZ"] == "" & as.numeric(VALUES[1, "M005_SZH"]) > 20){ 
      
      ORSZ <- "Z8"
      cat(paste("A", VALUES[1, "M003"], "törzsszám országkódja üresről Z8 lett, mert a megyekód (M005_SZH = ", VALUES[1, "M005_SZH"], ") > 20.", sep = " "))
      
    }
    
      #2023.09.28. A 15849038 törzsszám országkódja üresről HU lett, mert a megyekód (M005_SZH =  13 ) < 21.
    
      
  }
  
  if(VALUES[1, "LETSZAM"] == ""){
    
    LETSZAM <- 'N/A'
    
  }else{
    
    LETSZAM <- VALUES[1, "LETSZAM"]
    
  } 
  
  if(VALUES[1, "ARBEV"] == ""){
    
    ARBEV <- 'N/A'
    
  }else{
    
    ARBEV <- VALUES[1, "ARBEV"]
    
  } 
  
  
  if(nchar(VALUES[1, "M0581"]) == 3){
    
    M0581 <- paste("0", VALUES[1, "M0581"], sep = "")
    
  }else{
    
    M0581 <- VALUES[1, "M0581"]
    
  }
  
  M0581_H <- VALUES[1, "M0581_H"]
  
  if(CHANGED_ALL_M003[row] %in% CHANGED_M0582_R$M003){
    
    M0582 <- sqlQuery(channelOracle, paste("select M0582, TO_CHAR(M0582_H, 'YYYYMMDD') M0582_H from VB.F003_M0582 where M003 = '", CHANGED_ALL_M003[row], "' and M0582_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') order by KULDES_VEGE"))
    M0582 <- M0582[1, ]
    M0581 <- M0582$M0582
    
    if(is.na(M0581) == FALSE & nchar(M0581) == 3){
      
      M0581 <- paste("0", M0581, sep = "")
      
    }
    
    M0581_H <- M0582$M0582_H
    
    
  }

  
  if(CHANGED_ALL_M003[row] %in% HATALYOS$M003 & !(CHANGED_ALL_M003[row] %in% CHANGED_M0582_R$M003)){
    
    M0581 <- HATALYOS[HATALYOS$M003 == CHANGED_ALL_M003[row], "M0582"]
    
    if(is.na(M0581) == FALSE & nchar(M0581) == 3){
      
      M0581 <- paste("0", M0581, sep = "")
      
    }
    
    M0581_H <- HATALYOS[HATALYOS$M003 == CHANGED_ALL_M003[row], "M0582_H"]
    
  }
  
  
  if(CHANGED_ALL_M003[row] %in% HATALYVEGE$M003 & !(CHANGED_ALL_M003[row] %in% CHANGED_M0582_R$M003)){
    
    #& HATALYVEGE$M0582 == M0581
    if(length(HATALYVEGE[HATALYVEGE$M003 == CHANGED_ALL_M003[row], "M0582_HV"]) != 0){

      if(as.numeric(max(HATALYVEGE[HATALYVEGE$M003 == CHANGED_ALL_M003[row], "M0582_HV"])) > as.numeric(M0581_H)){
        
        M0581_H <- max(HATALYVEGE[HATALYVEGE$M003 == CHANGED_ALL_M003[row], "M0582_HV"])
        
      }
      
      
    }
    
  }
  
  
    
  if (grepl("\"", VALUES[1, "NEV"], fixed = TRUE) || grepl("'", VALUES[1, "NEV"], fixed = TRUE)  || grepl(",", VALUES[1, "NEV"], fixed = TRUE)){
    
    NEV <- paste("\"", VALUES[1, "NEV"], "\"", sep = "")
    NEV <- paste("\"", gsub("\"", "\"\"", VALUES[1, "NEV"]), "\"", sep = "")
    
  }else{
    
    NEV <- VALUES[1, "NEV"]#6. attribútum
  }
  
  
  if (grepl("\"", VALUES[1, "RNEV"], fixed = TRUE) || grepl("'", VALUES[1, "RNEV"], fixed = TRUE) || grepl(",", VALUES[1, "RNEV"], fixed = TRUE)){
    
    RNEV <- paste("\"", VALUES[1, "RNEV"], "\"", sep = "")
    RNEV <- paste("\"", gsub("\"", "\"\"", VALUES[1, "RNEV"]), "\"", sep = "")
    
  }else{
    
    RNEV <- VALUES[1, "RNEV"]
  }
  
  
  if (grepl("\"", VALUES[1, "TELNEV_SZH"], fixed = TRUE) || grepl("'", VALUES[1, "TELNEV_SZH"], fixed = TRUE) || grepl(",", VALUES[1, "TELNEV_SZH"], fixed = TRUE)){
    #|| grepl(".", VALUES[1, "TELNEV_SZH"], fixed = TRUE)
    if (grepl("\"", VALUES[1, "TELNEV_SZH"], fixed = TRUE) || grepl(",", VALUES[1, "TELNEV_SZH"], fixed = TRUE)){
      
      TELNEV_SZH <- substring(VALUES[1, "TELNEV_SZH"], 1, (nchar(VALUES[1, "TELNEV_SZH"]) - 2))
      
    }else{
      
      TELNEV_SZH <- VALUES[1, "TELNEV_SZH"]
      
    }
    
    TELNEV_SZH <- paste("\"", TELNEV_SZH, "\"", sep = "")
    cat(paste("Vesszőt tartalmazott a TELNEV_SZH: ", VALUES[1, "M003"], sep = ""))
    cat("\n")
    #Vesszőt tartalmazott a TELNEV_SZH: 30470547
    #2023.07.19.
    
    
  }else{
    
    TELNEV_SZH <- VALUES[1, "TELNEV_SZH"]
  }
  
  
  
  if (grepl("\"", VALUES[1, "UTCA_SZH"], fixed = TRUE) || grepl("'", VALUES[1, "UTCA_SZH"], fixed = TRUE) || grepl(",", VALUES[1, "UTCA_SZH"], fixed = TRUE)){
    
    UTCA_SZH <- paste("\"", VALUES[1, "UTCA_SZH"], "\"", sep = "")
    UTCA_SZH <- paste("\"", gsub("\"", "\"\"", VALUES[1, "UTCA_SZH"]), "\"", sep = "")
    
  }else{

    UTCA_SZH <- VALUES[1, "UTCA_SZH"]
  }
  
  
  if (grepl("\"", VALUES[1, "UTCA_LEV"], fixed = TRUE) || grepl("'", VALUES[1, "UTCA_LEV"], fixed = TRUE) || grepl(",", VALUES[1, "UTCA_LEV"], fixed = TRUE)){
    
    UTCA_LEV <- paste("\"", VALUES[1, "UTCA_LEV"], "\"", sep = "")
    UTCA_LEV <- paste("\"", gsub("\"", "\"\"", VALUES[1, "UTCA_LEV"]), "\"", sep = "")
    
  }else{
    
    UTCA_LEV <- VALUES[1, "UTCA_LEV"]
  }
  
  ALAKDAT <- VALUES[1, 29]
  if(VALUES[1, "M003"] == "15302724" || VALUES[1, "M003"] == "15736527"){
    
    ALAKDAT <- "19830101"
    print("MNB vagy KSH alakulás dátuma 1983. január 01-re változott")
    
  }
  
  if(nrow(CHANGED_HIST_ALAKDAT) > 0){
    
    if(CHANGED_ALL_M003[row] %in% CHANGED_HIST_ALAKDAT$M003){
      
      ALAKDAT <- gsub("-", "" ,CHANGED_HIST_ALAKDAT[CHANGED_HIST_ALAKDAT$M003 == CHANGED_ALL_M003[row], "ALAKDAT_U"])
      cat(paste("Az alakulás dátuma megváltozott:", VALUES[1, "M003"], VALUES[1, 29], ALAKDAT, sep = " "))
      ##Az alakulás dátuma megváltozott: 32295126 20230601 20230601 2023. június 06.
    }
    
  }
  
  
#M049_R volt M003_R helyett
 # str(VALUES)
  Q01[row, 4] <- paste("Q01", datum, "15302724", datum, "E", "KSHTORZS", paste("@KSHTORZS", KSHTORZS, sep = ""), VALUES[1, "M003"], VALUES[1, "M0491"], VALUES[1, "M0491_H"], M005_SZH, VALUES[1, "M005_SZH_H"], NEV, VALUES[1, "NEV_H"], RNEV, VALUES[1, "RNEV_H"], VALUES[1, "M054_SZH"], TELNEV_SZH, UTCA_SZH, VALUES[1, "SZEKHELY_H"], VALUES[1, "M054_LEV"], VALUES[1, "TELNEV_LEV"], UTCA_LEV, VALUES[1, "LEVELEZESI_R"], VALUES[1, 18], VALUES[1, 19], PFIOK_LEV, VALUES[1, 21], VALUES[1, 22], VALUES[1, 23], M025, VALUES[1, 25],  VALUES[1, 26], ARBEV_H, M009_SZH_CDV_TOGETHER, ALAKDAT, M0781, VALUES[1, 31], M058_J, VALUES[1, 33], VALUES[1, 34], MP65_H, VALUES[1, "UELESZT"], VALUES[1, "M003_R"], VALUES[1, "DATUM"], M0581, M0581_H, CEGV, VALUES[1, "CEGV_H"], VALUES[1, "MVB39"], VALUES[1, "MVB39_H"], ORSZ, LETSZAM, ARBEV, sep = ",")
  #VALUES[1, 44],
  #if (row == 1000) break
  
}
View(Q01)

odbcClose(channelOracle)

hiba <- 0
for(i in 1:nrow(Q01)){
  
  for(j in 1:ncol(Q01)){
    
    if(Q01[i, j] != CHANGED_ON_20240417[CHANGED_ON_20240417$FILENAME == Q01[i, 2] & CHANGED_ON_20240417$SORSZAM == Q01[i, 3], j]){
      
      hiba <- hiba + 1
      #i, j, 
      cat(paste(Q01[i, j], CHANGED_ON_20240417[CHANGED_ON_20240417$FILENAME == Q01[i, 2] & CHANGED_ON_20240417$SORSZAM == Q01[i, 3], j], sep = "\n"))
      cat("\n")
      cat("\n")
    }
    
  }
  #if (i == 1) break
  
}
hiba


Q01[substr(Q01$X4, 60, 67) == "20194389", 4]
Q01[substr(Q01$X4, 60, 67) == "21122387", 4]
Q01[substr(Q01$X4, 60, 67) == "21196494", 4]
Q01[substr(Q01$X4, 60, 67) == "23545784", 4]
Q01[substr(Q01$X4, 60, 67) == "26881229", 4]
Q01[substr(Q01$X4, 60, 67) == "26881298", 4]
Q01[substr(Q01$X4, 60, 67) == "30003453", 4]
Q01[substr(Q01$X4, 60, 67) == "30008764", 4]
Q01[substr(Q01$X4, 60, 67) == "30044405", 4]

CHANGED_ON_20240314[substr(CHANGED_ON_20240314$REKORD, 60, 67) == "11090081", 4]


nchar(Q01[substr(Q01$X4, 60, 67) == "10438310", 4])#510
max(nchar(Q01$X4))
CHANGED_ON_20230203[substr(CHANGED_ON_20230828$REKORD, 60, 67) == "24932161", 4]
View(nchar(VALUES[1, ]))

Q01[12, 4]


#Q01[substr(Q01$X4, 60, 67) == "15402989", 4]
#CHANGED_ON_20220819[substr(CHANGED_ON_20220819$REKORD, 60, 67) == "15402989", 4]
#Az adatbázis táblában szereplő ? jel #-re változott a szöveges állományban


Q01_SENT <- read.delim2(file = "Q:/mnb_ebead/elkuldott/Q014020715302724", header = FALSE)
str(Q01_SENT)
View(Q01_SENT)

Q01_REKORD <- Q01$X4

Q01_REKORD <- gsub(",\"", ",", Q01_REKORD)
Q01_REKORD <- gsub("\",", ",", Q01_REKORD)
Q01_REKORD <- gsub("\"\"", "\"", Q01_REKORD)
Q01_REKORD <- gsub("Ą", "Ľ", Q01_REKORD)
Q01_REKORD <- gsub("©", "Š", Q01_REKORD)
write.table(Q01_REKORD, "Q013101815302724", sep="\n", row.names=FALSE, col.names = FALSE, quote = FALSE)


hiba <- 0
for(i in 1:length(Q01_REKORD)){
  
  if(Q01_REKORD[i] != Q01_SENT[i, 1]){
    
    hiba <- hiba + 1
    cat(paste(Q01_REKORD[i], Q01_SENT[i, 1], sep = "\n"))
    cat("\n")
    cat("\n")
  }
  
  #if (i == 10) break
  
}
hiba

#Q01_REKORD[3]
#Q01_SENT[3, 1]

#Q0102 <- rbind(Q01, Q02)
#View(Q0102)
Q01_RENDEZVE <- CHANGED_ON_20231017[order(CHANGED_ON_20231017$SORSZAM), ]
View(Q01_RENDEZVE[Q01_RENDEZVE$KOD == "Q01", "REKORD"])
write.table(Q01_RENDEZVE[Q01_RENDEZVE$KOD == "Q01", "REKORD"], "Q013101715302724", sep="\n", row.names=FALSE, col.names = FALSE, quote = FALSE)
write.table(CHANGED_ON_20231017[CHANGED_ON_20231017$KOD == "Q02", "REKORD"], "Q023101715302724", sep="\n", row.names=FALSE, col.names = FALSE, quote = FALSE)
