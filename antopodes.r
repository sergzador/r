

#NOTE: in production I'd rather stage data scrapped from WEB into SLQ Server DB and work out delta in the DB. This solution is simpler and more portable. But for this particular test I work out delta in R.


library(rvest)
library(stringr)
library(dplyr)
library(rodbc)

url <- "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker=BKLN"

val_from_xpath <- function(url, xpth) {
    # extracts text from web page element based on web page url and xpath
    value <- url %>%
    read_html() %>%
    html_nodes(xpath=xpth) %>%
    html_text2()
    # cleans text by removing line breaks and extra whitespaces
    clean_text <-tolower(str_squish(gsub("[\n\t\r*]","",value)))[1] #[1]in case returns list
    return(clean_text)
      }

num_from_val <-function(text) {
# converts text into number after removing per cent, dollar sign and asterisk
number  <- as.numeric(gsub("[%$]","",text))

#if value has per cent sign, devide by 100
if (grepl('%',text)) {
  number <- number/100
}
return(number)
}
  
connect_mssql<- function(server,username,password) {
  #connect to SQL Server DB
conn <- odbcDriverConnect(paste("Driver={ODBC Driver 17 for SQL Server};Server=",server,";UID=",username,";PWD=",password,sep=''))
return(conn)}

replace_na <- function(inp) {
  # this function is used to make NA comparable to strings and avoiding installing extra library like tidyr
  if (is.na(inp)) {
    inp <-'none'}
    return(inp)
  }

#create dataframe with xpath for every table name
df_table_xpath <- data.frame(table_name=character(), 
                            xpath=character(),   
                            table_name_validation=character())

#populate table xpath dataframe (easier to maintain this way)
df_table_xpath[nrow(df_table_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/h3', 'bkln intraday stats')
df_table_xpath[nrow(df_table_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/h3/text()', 'yield')
df_table_xpath[nrow(df_table_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/h3', 'prior close')
df_table_xpath[nrow(df_table_xpath)+1, ] <- c('fund characteristics', '//*[@id="overview-details"]/div[6]/h3', 'fund characteristics')


#list all tables
all_tables_lst <-  df_table_xpath[, 'table_name']

#list of tables that passes validation
valid_tables_lst <- c()

#table name validation with saving to the list of tables passing validation 
for (tb in all_tables_lst) {
    xpath <- filter(df_table_xpath,  table_name == tb)['xpath'][1, 1]
    
    table_name_vailid <-filter(df_table_xpath,  table_name == tb)['table_name_validation'][1, 1]
    
    table_name_web <-val_from_xpath(url,  xpath)
    
    if (table_name_web == table_name_vailid) {
        valid_tables_lst <- append(valid_tables_lst,  tb)
    } 
    else {
        print(paste(tb, "have not passed validation"))
    }
}
#summary of table name validation
print(paste('Total number of tables:',length(all_tables_lst),'Number of tables passed table_name validation',length(valid_tables_lst)))

#create dataframe with xpath for every column name, table_name values MUST match with df_table_xpath
df_column_xpath <- data.frame(table_name=character(),  #from df_table_xpath
                             field_name_xpath=character(),   
                             field_name_validation=character(),  # used for validation and as field name
                             field_value_xpath=character(), 
                             stringsAsFactors=FALSE)

#populate table xpath dataframe (easier to maintain this way)
# in production I'd rather sourced this from Docker volume
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/ul/li[1]/text()', 'last trade', '//*[@id="overview-details"]/div[1]/ul/li[1]/span')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/ul/li[2]/span[1]/text()', 'current iiv', '//*[@id="overview-details"]/div[1]/ul/li[2]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[1]/ul/li[3]/text()', 'change', '//*[@id="overview-details"]/div[1]/ul/li[3]/span/text()')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('intraday', '//*[@id="overview-details"]/div[2]/ul/li/span[1]', 'nav at market close', '//*[@id="overview-details"]/div[2]/ul/li/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/ul/li[1]/span[1]/text()', 'sec 30 day yield', '//*[@id="overview-details"]/div[3]/ul/li[1]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/ul/li[2]/span[1]/text()', 'distribution rate', '//*[@id="overview-details"]/div[3]/ul/li[2]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/ul/li[3]/span[1]/text()', '12 month distribution rate', '//*[@id="overview-details"]/div[3]/ul/li[3]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('yield', '//*[@id="overview-details"]/div[3]/ul/li[4]/span/text()', '30-day sec unsubsidized yield', '//*[@id="overview-details"]/div[3]/ul/li[4]/div/div[1]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/ul/li[1]/span[1]', 'closing price', '//*[@id="overview-details"]/div[4]/ul/li[1]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/ul/li[2]/span[1]/text()', 'bid/ask midpoint', '//*[@id="overview-details"]/div[4]/ul/li[2]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/ul/li[3]/span[1]/text()', 'bid/ask prem/disc', '//*[@id="overview-details"]/div[4]/ul/li[3]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[4]/ul/li[4]/span[1]', 'bid/ask prem/disc', '//*[@id="overview-details"]/div[4]/ul/li[4]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('prior close', '//*[@id="overview-details"]/div[5]/ul/li/span[1]/text()', 'median bid/ask spread', '//*[@id="overview-details"]/div[5]/ul/li/span[2]')
#df_column_xpath[nrow(df_column_xpath)+1, ] <- c('fund characteristics', '//*[@id="overview-details"]/div[6]/ul[1]/li[1]/span[1]/text()', 'yield to maturity', '//*[@id="overview-details"]/div[6]/ul[1]/li[1]/span[2]')

df_column_xpath[nrow(df_column_xpath)+1, ] <- c('fund characteristics', '//*[@id="overview-details"]/div[6]/ul[2]/li[1]/span[1]/text()', '3 month libor', '//*[@id="overview-details"]/div[6]/ul[2]/li[1]/span[2]')
df_column_xpath[nrow(df_column_xpath)+1, ] <- c('fund characteristics', '//*[@id="overview-details"]/div[6]/ul[2]/li[2]/span[1]/text()', 'weighted avg price', '//*[@id="overview-details"]/div[6]/ul[2]/li[2]/span[2]')


#list of tables that has not passed column name validation
invalid_tables_lst <-c()

#if field name extracted from xpath does not match expected field name, table name is added to list that would be excluded from loading into DB
for (i in seq(nrow(df_column_xpath))) {
    expected_field_name <-  df_column_xpath[i,]['field_name_validation'][1,1]
    xpath <- df_column_xpath[i,]['field_name_xpath'][1,1]
    field_name_web <- val_from_xpath(url, xpath) 
    table_name  <-  df_column_xpath[i,]['table_name'][1,1]

    if (expected_field_name != replace_na(field_name_web)) {
        invalid_tables_lst <- append(invalid_tables_lst, table_name)
        print(paste("Table [",table_name, "] has notpassed column name validation because [",expected_field_name,"] is not equal to [", field_name_web,"]"))
    }
}

#summary of column name validation
print(paste('Total number of tables:',length(valid_tables_lst),'Number of tables has NOT passed column_name validation',length(unique(valid_tables_lst))))

#extracing values from website, field_name_validation is used as column name for tables that passed table name and column name validation
##intraday table
if ('intraday' %in% valid_tables_lst & !('intraday' %in% invalid_tables_lst)) {
df_intraday_stats <- data.frame(last_trade=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == 'last trade' )['field_value_xpath'][1, 1] )),
                                current_iiv=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == 'current iiv' )['field_value_xpath'][1, 1] )), 
                                change=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == 'change' )['field_value_xpath'][1, 1] )), 
                                nav_at_market_close=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'intraday' & field_name_validation == 'nav at market closew' )['field_value_xpath'][1, 1] )))
}

##yield table
if ('yield' %in% valid_tables_lst & !('yield' %in% invalid_tables_lst)) {
df_yield <- data.frame(sec_30_day_yield=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'yield' & field_name_validation == 'sec 30 day yield' )['field_value_xpath'][1, 1] )),
                       distribution_rate=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'yield' & field_name_validation == 'distribution rate' )['field_value_xpath'][1, 1] )),
                       distribution_rate_12_month=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'yield' & field_name_validation == '12 month distribution rate' )['field_value_xpath'][1, 1] )),
                       sec_unsubsidized_yield_30_day=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'yield' & field_name_validation == '30-day sec unsubsidized yield' )['field_value_xpath'][1, 1] )))
}

##prior close table
if ('prior close' %in% valid_tables_lst & !('prior close' %in% invalid_tables_lst)) {
df_prior_close <- data.frame(closing_price=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'closing price' )['field_value_xpath'][1, 1] )),
                             bid_ask_midpoint=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'bid/ask midpoint' )['field_value_xpath'][1, 1] )),
                             bid_ask_prem_disc=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'bid/ask prem/disc' )['field_value_xpath'][1, 1] )),
                             bid_ask_prem_disc_pct=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'bid/ask prem/disc' )['field_value_xpath'][2, 1] )), # same field name in source, hence returns two line df
                             median_bid_ask_spread=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'prior close' & field_name_validation == 'median bid/ask spread' )['field_value_xpath'][1, 1] )))
}

##fund characteristics table
if ('fund characteristics' %in% valid_tables_lst & !('fund characteristics' %in% invalid_tables_lst)) {
df_fund_info <- data.frame(libor_3month=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'fund characteristics' & field_name_validation == '3 month libor' )['field_value_xpath'][1, 1] )),
                            weighted_avg_price=num_from_val(val_from_xpath(url, filter(df_column_xpath,  table_name == 'fund characteristics' & field_name_validation == 'weighted avg price' )['field_value_xpath'][1, 1] )))
}