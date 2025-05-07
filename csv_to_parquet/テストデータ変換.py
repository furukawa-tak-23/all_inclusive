import pyarrow.parquet as pq
import sys
from pyarrow import int16, int32, int64, float64, float32, bool_, date32, date64, decimal128, timestamp, string, Table, time32, parquet
from pyarrow.csv import read_csv, ReadOptions, ParseOptions, ConvertOptions

args = sys.argv

filename = args[1]

column_types = {
# pyarrowの型を定義（https://arrow.apache.org/docs/python/api/datatypes.html）
# 例) '{項目物理名1}' : string(), '{項目物理名2}' : int32(), '{項目物理名3}' : date32()
    
# 売上データ_日次
# '売上年月日': date32(),'売上品番': string(),'日数別売上数バラ': int64(),'日数別売上数バラ_最大値': int64(),'得意先コード帳合親': string(),'製品分類コード': string(),'分析基盤更新日時': timestamp('s'),'品番発売年月日': date32()

# FANT1200
# 'T_C_DCCD': string(), 'T_C_SKU': string(), 'T_I_DT_HISTORY': string(), 'T_I_QTY': int64(), 'T_I_OPENQTY': int64(), 'T_I_LOCATEDQTY': int64(),'T_I_SHIPPEDQTYSUM': int64(),'T_I_RECEIPTEDQTY': int64(),'T_I_ALLQTY': int64(),'T_I_ENABLEQTY': int64(),'T_I_ADMINQTY': int64(),'T_I_ORDERPOINT': int64(),'T_I_ORDERLOT': int64(),'T_I_MAXSTOCK': int64(),'T_I_SAFETYSTOCK': int64(),'T_I_LEFTPOQTY': int64(),'K_REC_CRT_ID': string(),'K_REC_CRT_DT': string(),'K_REC_CRT_TIM': string(),'K_REC_DLT_FLG': string(),'ANALYZE_UPD_DT': timestamp('s')

# FANT1260
# 'T_C_ORDERSKU': string(),'T_C_SKU': string(),'T_I_TRANSFERQTY': int64(),'T_C_SENDF': string(),'T_I_DT_ADD': string(),'T_C_TM_ADD': string(),'K_REC_CRT_ID': string(),'K_REC_CRT_DT': string(),'K_REC_CRT_TIM': string(),'K_REC_DLT_FLG': string(),'ANALYZE_UPD_DT': timestamp('s')

# FANT0340
# 'K_CTLG_NO': string(),'K_CTLG_PRT_NO': string(),'K_EFF_STRT_DT': int64(),'K_EFF_END_DT': int64(),'K_CTLG_TYP': string(),'GOODS_SORT_CD': string(),'SALE_PR': float32(),'K_SML_DELV_STNDRD_PR': float32(),'K_CMPN_PR': float32(),'K_PR_CHNG_TYP': string(),'K_TAX_EXC_SALE_PR': float32(),'K_TAX_INC_SALE_PR': float32(),'K_AGNT_SL_PR_RT': int64(),'K_AAGNT_RT': int64(),'K_AGNT_SL_PR': float32(),'K_AAGNT_SL_PR': float32(),'K_MEM_ADD_TAX_PR': float32(),'K_SML_DELV_STNDRD_ADD_TAX': float32(),'K_AGNT_ADD_TAX_PR': float32(),'K_AAGNT_ADD_TAX_PR': float32(),'K_ADD_TAX_RT': float32(),'K_CTLG_PG': int64(),'K_CTLG_PG_PRT_INFO': int64(),'K_REC_CRT_ID': string(),'K_REC_CRT_DT': int64(),'K_REC_CRT_TIM': string(),'K_REC_UPD_ID': string(),'K_REC_UPD_DT': int64(),'K_REC_UPD_TIM': string(),'K_REC_DLT_FLG': string(),'K_REC_UPD_CNTR': int64(),'ANALYZE_UPD_DT': timestamp('s')

# TMEMBER
# 'MEM_NO': int32(), 'MEM_NM_FST': string(), 'MEM_NM_MID': string(), 'MEM_NM_LAST': string(), 'MEM_NM_ETC': string(), 'MEM_ID': string(), 'MEM_PWD': string(), 'PWD_QST1': string(), 'PWD_ANS1': string(), 'PWR_ACC_KEY': string(), 'PWR_JSESSID': string(), 'PWR_URL_EXPIRE': string(), 'MEM_LVL_CD': string(), 'MEM_EML': string(), 'EML_YN': string(), 'MEM_TP': string(), 'IDN_NO': string(), 'CUR_POINT': int32(), 'REC_MEM_NO': int32(), 'SEX_TP': string(), 'SOL_LUN_CL': string(), 'BIRTH_DT': date32(), 'WED_YN': string(), 'WED_ANNV_DT': date32(), 'REG_DM': string(), 'UPD_DM': string(), 'USE_YN': string(), 'USER_ID': string(), 'MEM_EML_MBL': string(), 'HTML_YN': string(), 'MB_UID': string(), 'INGESTION_DATE': date32()

# TACD0090
# 'MEM_NO': int32(), 'K_MEM_CLS_DT_TYP': string(), 'K_3YR_AGO_GAIN_PONT': int32(), 'K_2YR_AGO_GAIN_PONT': int32(), 'K_1YR_AGO_GAIN_PONT': int32(), 'K_CYR_GAIN_PONT': int32(), 'K_LMNTH_STKD_PONT': int32(), 'K_NEW_MEM_PONT': int32(), 'K_CMNTH_INTRO_CNT': int32(), 'K_CMNTH_INTRO_PONT': int32(), 'K_CMNTH_BYNG_PONT': int32(), 'K_CMNTH_GAIN_PONT': int32(), 'K_CMNTH_EXCHNGE_PONT': int32(), 'K_CMNTH_STKD_PONT': int32(), 'K_FIR_P_PONT_DT': date32(), 'K_REC_CRT_ID': string(), 'K_REC_CRT_DT': date32(), 'K_REC_CRT_TIM': time32('ms'), 'K_REC_UPD_ID': string(), 'K_REC_UPD_DT': date32(), 'K_REC_UPD_TIM': time32('ms'), 'K_REC_DLT_FLG': string(), 'K_REC_UPD_CNTR': int32(), 'INGESTION_DATE': date32()

# # # TCUD0150
# 'MEM_NO': int32(), 'K_MSTR_NO': string(), 'K_FIR_HSTRY_REG_STS': string(), 'K_ORD_STP_RSN_TYP': string(), 'K_ML_REFSL_FLG': string(), 'K_DM_REFSL_FLG': string(), 'K_CTLG_REFSL_FLG': string(), 'K_LNG_CMMT': string(), 'K_EXTNSN_NO': string(), 'K_SECNDRY_MEM_ML': string(), 'K_SECNDRY_MEM_TEL_NO': string(), 'K_SECNDRY_MEM_FAX_NO': string(), 'K_SECNDRY_MEM_ZIP_NO': string(), 'K_SECNDRY_MEM_ADDR_1': string(), 'K_SECNDRY_MEM_ADDR_2': string(), 'K_SECNDRY_MEM_ADDR_3': string(), 'K_SECNDRY_MEM_ADDR_4': string(), 'K_SECNDRY_ADDR_CD': string(), 'K_SPE_REG_VIA_TYP': string(), 'K_NON_DELV_MEM_FLG': string(), 'K_MEM_WRK_DT_TYP': string(), 'K_OCRTN_RECYCL_FLG': string(), 'K_SHCK_ABSORB_NON_FLG': string(), 'K_SMPL_PCKG_RQST_FLG': string(), 'K_DIV_DELV_NON_FLG': string(), 'K_MEM_SPR_CLM_FLG1': string(), 'K_MEM_SPR_CLM_FLG2': string(), 'K_MEM_SPR_CLM_FLG3': string(), 'K_MEM_SPR_CLM_FLG4': string(), 'K_MEM_SPR_CLM_FLG5': string(), 'K_MEM_SPR_CLM_FLG6': string(), 'K_MEM_SPR_CLM_FLG7': string(), 'K_MEM_SPR_CLM_FLG8': string(), 'K_MEM_SPR_CLM_FLG9': string(), 'K_MEM_SPR_CLM_FLG10': string(), 'K_MEM_SPR_CLM_TYP1': string(), 'K_MEM_SPR_CLM_TYP2': string(), 'K_MEM_SPR_CLM_TYP3': string(), 'K_MEM_SPR_CLM_TYP4': string(), 'K_MEM_SPR_CLM_TYP5': string(), 'K_MEM_SPR_CLM_TYP6': string(), 'K_MEM_SPR_CLM_TYP7': string(), 'K_MEM_SPR_CLM_TYP8': string(), 'K_MEM_SPR_CLM_TYP9': string(), 'K_MEM_SPR_CLM_TYP10': string(), 'K_MEM_SPR_CLM1': string(), 'K_MEM_SPR_CLM2': string(), 'K_MEM_SPR_CLM3': string(), 'K_MEM_SPR_CLM4': string(), 'K_MEM_SPR_CLM5': string(), 'K_MEM_SPR_CLM6': string(), 'K_MEM_SPR_CLM7': string(), 'K_MEM_SPR_CLM8': string(), 'K_MEM_SPR_CLM9': string(), 'K_MEM_SPR_CLM10': string(), 'K_REC_CRT_ID': string(), 'K_REC_CRT_DT': date32(), 'K_REC_CRT_TIM': time32('ms'), 'K_REC_UPD_ID': string(), 'K_REC_UPD_DT': date32(), 'K_REC_UPD_TIM': time32('ms'), 'K_REC_DLT_FLG': string(), 'K_REC_UPD_CNTR': int32(), 'INGESTION_DATE': date32()

# # # TCUD0030
# 'MEM_NO': int32(), 'K_MEM_NM': string(), 'K_MEM_KNM': string(), 'K_MEM_DVN_NM': string(), 'K_MEM_DVN_KNM': string(), 'K_MEM_INCHRGE_NM': string(), 'K_MEM_INCHRGE_KNM': string(), 'K_MEM_MAIN_NM': string(), 'K_MEM_MAIN_KNM': string(), 'K_MEM_TEL_NO': string(), 'K_MEM_FAX_NO': string(), 'K_MEM_ZIP_NO': string(), 'K_MEM_ADDR_1': string(), 'K_MEM_ADDR_2': string(), 'K_MEM_ADDR_3': string(), 'K_MEM_ADDR_4': string(), 'K_ADDR_CD': string(), 'K_PAY_TYP': string(), 'K_BNK_CD': string(), 'K_BANK_BRNCH_CD': string(), 'K_BNK_ACCT_TYP': string(), 'K_BNK_ACCT_NO': string(), 'K_MEM_BNK_HLDR_NM': string(), 'K_MEM_BNK_HLDR_KNM': string(), 'K_MEM_CLS_DT_TYP': string(), 'K_ACCT_TRNSFR_FLG': string(), 'K_ORD_STS_TYP': string(), 'K_KNET_FMLY_CD': string(), 'K_KNET_FMLY_VIA_TYP': string(), 'K_KNET_FMLY_REG_TYP': string(), 'K_KNET_FMLY_PSN_TYP': string(), 'K_BLL_ADDR_TYP': string(), 'K_BLL_ADDR_MEM_NO': int32(), 'K_IDNTFY_TYP': string(), 'K_IDNTFY_MEM_NO': int32(), 'K_CTLG_DELV_ADDR_TYP': string(), 'K_CTLG_DELV_STS_TYP': string(), 'K_RVSD_CTLG_DELV_TYP': string(), 'K_REG_MDA_TYP': string(), 'K_INTRO_MEM_NO': int32(), 'K_REG_ASSM_DT': date32(), 'K_UPD_ASSM_DT': date32(), 'K_REC_CRT_ID': string(), 'K_REC_CRT_DT': date32(), 'K_REC_CRT_TIM': time32('ms'), 'K_REC_UPD_ID': string(), 'K_REC_UPD_DT': date32(), 'K_REC_UPD_TIM': time32('ms'), 'K_REC_DLT_FLG': string(), 'K_REC_UPD_CNTR': int32(), 'INGESTION_DATE': date32()

# # # TORD0110
# 'MEM_NO': int32(), 'K_FIR_P_ORD_DT': date32(), 'K_CRDT_LMT_AMT': int32(), 'K_CMNTH_SL_TOT': int32(), 'K_REC_CRT_ID': string(), 'K_REC_CRT_DT': date32(), 'K_REC_CRT_TIM': time32('ms'), 'K_REC_UPD_ID': string(), 'K_REC_UPD_DT': date32(), 'K_REC_UPD_TIM': time32('ms'), 'K_REC_DLT_FLG': string(), 'K_REC_UPD_CNTR': int32(), 'INGESTION_DATE': date32()

# TMDD0400
'C_ORDERSKU': string(), 'C_SKU': string(), 'I_TRANSFERQTY': int32(), 'C_SENDF': string(), 'C_DELETEF': string(), 'DT_ADD': timestamp('s'), 'DT_UPDATE': timestamp('s'), 'L_REC_CRT_ID': string(), 'L_REC_CRT_DT': int32(), 'L_REC_CRT_TIM': string(), 'L_REC_UPD_ID': string(), 'L_REC_UPD_DT': int32(), 'L_REC_UPD_TIM': string(), 'L_REC_UPD_CNTR': int32(), 'INGESTION_DATE': date32()



}

convertoptions = ConvertOptions(
        check_utf8=True,  # 文字列カラムのUTF-8妥当性をチェック
        column_types=column_types,  # 列のデータ型を辞書型で渡す
        null_values=['']  # データ内のNULLを表す文字列
    )
pyarrow_table = read_csv(
        'csv/' + filename + '.csv',
        convert_options=convertoptions
    )

pq.write_table(pyarrow_table, 'parquet/' + filename + '.parquet')
