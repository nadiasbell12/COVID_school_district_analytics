library(lubridate)
library(tidyverse)
library(readxl)
library(openxlsx)
library(data.table)


AL_closures <- read_excel('Downloads/AL - Calendar.2020.ALL.2020.05.12.11.15.xlsx')
IL_closures <- read.csv("Downloads/IL - CrystalReportViewer1.csv", header = F)
KY_closures <- read_excel("Downloads/KY - 2019-20OriginalCalendarSummary.xlsx")
NH_closures <- read.csv("Downloads/NH - district-calendars19-20.csv") 
RI_closures <- read_excel("Downloads/RI - Public School Calendar .xlsx")
FL_closures <- read.xlsx("Downloads/FL - 1920SYCSW.xlsx", detectDates = T)
GA_closures <- read.xlsx(("Downloads/GA - Copy of School Calendars.xlsx"), detectDates = T) 
IN_closures <- read.xlsx("Downloads/IN - 2019-2020 School Calendar 05142020.xlsx", detectDates = T)
SD_closures <- read.xlsx("Downloads/SD - PubEndOfYear1920.xlsx", detectDates = T)
MO_closures <- read.xlsx("Downloads/MO - 2020 Planned Calendar.xlsx", detectDates = T)
MT_closures <- read.xlsx("Downloads/MT - FY2020 First and Last Day of School.xlsx", sheet = "endDates", detectDates = T)
WI_closures <- read.xlsx("Downloads/WI - 2019-2020_School_Calendar (1).xlsx", sheet = "2019-2020 Calendar", detectDates = T)
MD_closures <- read.xlsx("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/Maryland_School_Dates.xlsx",
                        detectDates = T) 
WV_closures <- read.xlsx("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/West_Virginia_School_Dates.xlsx",
                         detectDates = T) 
VA_closures <- read.xlsx("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/Virginia_School_Dates.xlsx",
                         detectDates = T)
SC_closures <- read.xlsx("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/South_Carolina_School_Dates.xlsx",
                         detectDates = T)
UT_closures <- read.xlsx("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/Utah_School_Dates_2.xlsx",
                         detectDates = T)

HI_closures <- read.xlsx("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/Hawaii_School_Dates.xlsx",
                         detectDates = T)
DE_closures <- read.xlsx("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/Delaware_School_Dates_detailed.xlsx",
                         detectDates = T)
#CA_closures <- read.xlsx("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/California_inferred_School_Dates.xlsx",
                         #detectDates = T)
#-------------------- data cleaning -----------------------------
extra = regex(" County| Independent| Schools| School| Cmnty| Unit| Charter| Unified| District| Public", ignore_case = T)
#### easily extractable data ####



# fine to not edit names for this but change in closure date data to get rid of "School District" string
AL_closures <- AL_closures[,1:5]
AL_closures <- AL_closures %>% select(district = `System Name`, districtID =`System Code`, endDate = `Closed Date`) %>% mutate(districtID = as.character(districtID),
                                                                                                                               state = rep("AL", nrow(AL_closures)),
                                                                                                    orig_district_instr = district,
                                                                                                    district = toupper(district))

AL_school_ID <- AL_school_ID[-(1:2),c(2,3,4,18)]
names(AL_school_ID) <- c("System Code", "School Code", "School Name", 
                         "NCES_ID")

## note: how to deal with abbrevations? instruction dates CUSD/SD abbreviation while closure date data isn't 
## need to remove all_districts superfluous information 
IL_closures <- IL_closures %>% select(district = V8, endDate = V10) %>% mutate(state = rep("IL", nrow(IL_closures)),
                                                                               orig_district_instr = as.character(district), 
                                                                               district = toupper(str_remove_all(district, " CUSD| SD| HSD| USD| CCSD| CHSD| UD| CUD")))



## note: differences in district names, all_districts "Adair County School District" vs. KY closures "Adair County" 
KY_closures <- KY_closures %>% select(district = `District Name`, districtID = `District Number`, endDate = `Students Last Day`) %>% mutate(districtID = as.character(districtID),
                                                                                                                                            state = rep("KY", nrow(KY_closures)), 
                                                                                                            orig_district_instr = district, 
                                                                                                            district = toupper(str_remove_all(district, " County| Independent")))

NH_closures <- NH_closures[-(1:10), -1] 
NH_closures <- NH_closures %>% select(district = X,  endDate = X.25) %>% 
  mutate(state = rep("NH", nrow(NH_closures)),
         orig_district_instr = district,
         district = toupper(district),
         endDate = as.Date(paste0(endDate, "/2020"),format='%m/%d/%Y'))

## note: district names already clean here, need to remove excess from all_districts 
RI_closures <- RI_closures %>% select("district" = `DISTRICT, CHARTER, OR STATE SCHOOL`,
                                      "endDate" =`LAST DAY OF SCHOOL` ) %>% mutate(state = rep("RI", nrow(RI_closures)),
                                                                                   orig_district_instr = district,
                                                                                   district = toupper(district))

names(FL_closures) <- FL_closures[4,]
FL_closures <- FL_closures[-(1:4), c(1,5)] 
FL_closures <- FL_closures %>% select("district" = "DISTRICT","endDate" = "CLOSE") %>% 
  mutate(state = rep("FL", nrow(FL_closures)), 
         orig_district_instr = district, 
         district = toupper(district))

# KY_closures <- KY_closures[,c(1,2,3,6)] %>% rename("district" = "District Name") %>% 
#   mutate(state = rep("KY", nrow(KY_closures)), 
#          orig_district_instr = district,
#          district = toupper(district))

GA_closures <- GA_closures %>% select(district = School.District, endDate = Last.Day.of.School) %>% mutate(state = "GA", 
                                                                                                            orig_district_instr = district, 
                                                                                                           district = toupper(str_remove_all(district, " County| City| Public| Schools| School ")))
#Q: different schools within same district have different end dates, which do we chose for the district? 
# IN_closures %>% filter(Calendar.Type == "Student Calendar") %>% group_by(Corporation.Name) %>% summarize(disc = n_distinct(End.Date) > 1) %>% filter(disc == 1)
IN_closures <- IN_closures %>% filter(Calendar.Type == "Student Calendar") %>% 
  select(district = Corporation.Name, districtID = IDOE.Corp.ID, endDate = End.Date) %>% 
                                                        mutate(districtID = as.character(districtID),
                                                               state = "IN", 
                                                           orig_district_instr = district, 
                                                           district = toupper(str_remove_all(district, regex(" Community| Schools| School| County| Corporation|Corp| Schls| Sch| Com", ignore.case = T)))) %>% 
  unique 


SD_closures <- SD_closures %>% select(district =District.Name, districtID = District.Number, endDate = End.of.Year) %>% mutate(districtID = as.character(districtID),
                                                                                                                               state = "SD", 
                                                                                  orig_district_instr = district, 
                                                                                  district = toupper(district))

# 17 school systems that have different school closures within system 
#MT_closures %>% group_by(SchoolSystem) %>% summarize(disc = n_distinct(LastDayOfSchool) > 1) %>% filter(disc == 1)
MT_closures <- MT_closures %>% select(district = SchoolSystem, districtID = SS, endDate = LastDayOfSchool) %>%
  unique %>% 
  mutate(districtID = as.character(districtID),
         state = "MT", 
         orig_district_instr = district, 
         district = toupper(str_remove_all(district, " Elementary| Public| Schools| School| K-12| High")))

WI_closures <- WI_closures %>% select(district = X3, districtID=X2, endDate = X8) %>% mutate(districtID = as.character(districtID),
                                                                                             state = "WI", 
                                                              orig_district_instr = district, 
                                                              district = toupper(district))

# 36 districts that have differing dates
MO_closures <- MO_closures %>% select(district = Name, districtID = District.Code, endDate = Planned.End.Date) %>% unique %>% 
  mutate(districtID = as.character(districtID),
         state = "MO", 
         orig_district_instr = district, 
         district = toupper(str_remove_all(district, "Schools| School| Public| INC")))


#### pdf, misc webscraping states ####


MD_closures <- MD_closures %>% select(district = School.System, endDate) %>% mutate(state = "MD", 
                                                                     orig_district_instr = district,
                                                                     district = toupper(district))
WV_closures <- WV_closures %>% select(district = county, endDate) %>% mutate(state = "WV", 
                                                              orig_district_instr = district,
                                                              district = toupper(district))

VA_closures <- VA_closures %>% select(district = Division.Name, districtID = Div..No., endDate) %>% mutate(districtID = as.character(districtID),
                                                                                                           state = "VA", 
                                                                     orig_district_instr = district,
                                                                     district = toupper(str_remove_all(district, extra)))

SC_closures <- SC_closures %>% select(district = School_District, endDate) %>% mutate(state = "SC", 
                                                                                      orig_district_instr = district,
                                                                                      district = toupper(district))

UT_closures <- UT_closures %>% select(district = School_District, endDate) %>% mutate(state = "UT", 
                                                                                      orig_district_instr = district,
                                                                                      district = toupper(district))

HI_closures <- HI_closures %>% select(district = School_District, endDate) %>% mutate(state = "HI", 
                                                                                      orig_district_instr = district,
                                                                                      district = toupper(district))

DE_closures <- DE_closures %>% select(district = School_District, endDate, startGrades, endGrades)  %>% mutate(endDate = as.character(endDate),
                                                                                                               state = "DE", 
                                                                                                               orig_district_instr = district,
                                                                                                              district = toupper(district))

#### public holidays data states ####
scraped_states = c("Alaska", "Arizona", "Arkansas", "Colorado", "Connecticut", "Idaho",
                   "Louisiana", "Maine", "Nebraska", "North Carolina",
                   "North Dakota", "Oregon", "Tennessee", "Vermont", "Washington", 
                   "Wyoming", "New York", "Ohio", "Pennsylvania", "Texas",  "New Jersey", "Michigan", "Iowa",
                   "Massachusetts", "Minnesota", "Mississippi", "New Mexico", "Oklahoma", "Nevada", "Kansas", "California")
scraped_states = unique(scraped_states)

scraped_files = paste0("C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/raw files/state_instruction_dates/", 
                       scraped_states, "_School_Dates.xlsx")

state_names = state.abb[match(scraped_states, state.name)]


ca_state = read.xlsx(scraped_files[31], detectDates = TRUE)
#scraped_states_dat <- lapply(scraped_files, read.xlsx, detectDates = T)

#mapply(function(x,y) x <- x %>% mutate(state = y), x = scraped_states_dat, y = state_names)


#scraped_data <- mapply(function(x, y) read.xlsx(x, detectDates = TRUE)  %>%
#        mutate(state = y), x = scraped_files, y = state_names)

all_scraped_states <- NULL
for (i in 1:length(scraped_files)){
  state_dat <- read.xlsx(scraped_files[i], detectDates = TRUE)
  state_dat <- state_dat %>% mutate(state = state_names[i])
  all_scraped_states <- rbind(all_scraped_states,state_dat)
}

all_scraped_states <- all_scraped_states %>% select(district = X1, state, endDate) %>% mutate(orig_district_instr = district, 
                                                                                              endDate = as.character(endDate),
                                                                        district = toupper(str_remove_all(district, regex(" Community| Schools| School| County| Corporation| Corp| Schls| Sch| Com| Unified| Public| District| ISD| CISD", ignore.case = T))))


read.xlsx(scraped_files[6], detectDates = TRUE)


# Merge data together -----------------------------------------------------

                                                                                                  
states <- list(AL_closures, IL_closures, KY_closures, NH_closures, RI_closures, FL_closures, GA_closures, IN_closures, SD_closures, MT_closures,
               WI_closures, MD_closures, WV_closures, MO_closures, VA_closures, SC_closures, UT_closures, HI_closures)

states1 <- lapply(states, function(x) x <- x %>% mutate(endDate = as.character(endDate))) 

all_states_endDates <- do.call(bind_rows,states1) %>% bind_rows(all_scraped_states, DE_closures) 


write.csv(all_states_endDates, "C:/Users/nbell/Mathematica/HS COVID-19 Analytics - Documents/cleaned_files/COVID_district_instr_dates_distID.csv")
