library("RODBC")
channelOracle <- odbcDriverConnect(paste("DRIVER={Oracle in OraClient18Home1_32bit};DBQ=EMERALD.KSH.HU;UID=", Sys.getenv("userid"), ";PWD=", Sys.getenv("pwd")), DBMSencoding = "latin1")

#1. tábla
sqlSave(channel = channelOracle, dat = Q0102, tablename = "VB_REP.MNB_NAPI", append = TRUE, rownames = FALSE, colnames = FALSE, fast = FALSE)




odbcClose(channelOracle)
