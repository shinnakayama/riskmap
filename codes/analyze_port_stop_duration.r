library(lme4)
library(ggplot2)
library(plyr)

# output of query 'port_stop_duration.sql'
data <- read.csv('port_stop_duration.csv')

data <- data[!is.na(data$flag),]
data <- data[!is.na(data$vessel_class),]
data <- data[!is.na(data$port_iso3),]

# list of sovereign - territory pairs
# queried from GFW on March 29, 2021
# `world-fishing-827.gfw_research.eez_info`
eez <- read.csv('eez_info.csv')
eez <- eez[eez$eez_type=='200NM',]
eez <- eez[eez$territory1_iso3 != eez$sovereign1_iso3,]
pair <- unique(eez[, c('territory1_iso3', 'sovereign1_iso3')])
pair <- rbind(pair, c('MAC', 'CHN'))    # add Macao to China
pair <- rbind(pair, c('HKG', 'CHN'))    # add Hong Kong to China


# add sovereign nation to vessel flag and port
data$flag_x <- data$flag
data$port_iso3_x <-data$port_iso3
for(i in 1:nrow(pair)) {
  data$flag_x[data$flag==pair$territory1_iso3[i]] <- pair$sovereign1_iso3[i]
  data$port_iso3_x[data$port_iso3==pair$territory1_iso3[i]] <- pair$sovereign1_iso3[i]
}


# select visit by *foreign* vessels
data <- data[data$flag_x != data$port_iso3_x,]


# add flag group by Ford and Wilcox
group1 <- c('ATG','BRB','CYM','LBR','VCT','VUT')
group2 <- c('BHS','BHR','BLZ','BOL','BRN','KHM','CYP','GNQ','GAB','GEO','HND','KIR','MDG','MLT',
    'MHL','PAN','PRT','KNA','WSM','SLE','LKA','TON','TZA')
group3 <- c('ALB','DZA','AGO','AIA','ARG','AUS','AZE','BGD','BEL','BMU','BRA','BGR','CPV',
    'CMR','CAN','CHL','HKG','TWN','COL','COD','CRI','HRV','CUB','DNK','DJI','ECU',
    'EGY','ERI','EST','ETH','FJI','FIN','FRA','GMB','DEU','GHA','GRC','GRL','GRD',
    'GTM','GUY','ISL','IND','IDN','IRN','IRQ','IRL','ISR','ITA','JPN','JOR','KAZ',
    'KEN','PRK','KOR','KWT','LAO','LVA','LBN','LBY','LTU','LUX','MYS','MDV','MRT',
    'MUS','MEX','MNE','MAR','MOZ','MMR','NAM','NLD','NZL','NGA','NOR','OMN','PAK',
    'PNG','PRY','PER','PHL','POL','QAT','RUS','SAU','SEN','SYC','SGP','SVN','ZAF',
    'ESP','SDN','SUR','SWE','CHE','SYR','THA','TTO','TUN','TUR','TKM','UKR','ARE',
    'GBR','USA','URY','VEN','VNM','YEM')

data$flag_group <- 'other'
data$flag_group[data$flag=='CHN'] <- 'china'
data$flag_group[data$flag %in% group1] <- 'group1'
data$flag_group[data$flag %in% group2] <- 'group2'
data$flag_group[data$flag %in% group3] <- 'group3'


# remove port stop duration < 60 min
data <- data[data$duration_min >= 60,]


# model
data$duration_h_log <- log(data$duration_min/60)
model <- lmer(duration_h_log ~ 0 + flag_group + (1|port_iso3) + (1|vessel_class), data=data)

summary(model)


# get confidence intervals
nranpars <- length(getME(model, 'theta'))
nfixpars <- length(fixef(model))

c1 <- confint(model, method='boot', nsim=1000,
   parm=(nranpars+2):(nranpars+nfixpars+1), parallel='multicore', ncpus=2)

c1 <- as.data.frame(c1)
colnames(c1) <- c('lower', 'upper')
c1$mean <- fixef(model)
c1$flag <- rownames(c1)
c1 <- c1[order(c1$mean, decreasing=FALSE),]
c1$flag <- factor(c1$flag, levels=c1$flag)

write.csv(c1, 'data/port_stop/flag_ci_foreign.csv')

c1 <- read.csv('data/port_stop/flag_ci_foreign.csv')
myrange <- seq(12,48,2)

p <- ggplot(aes(x=flag, y=mean), data=c1) +
   geom_pointrange(aes(ymin=lower, ymax=upper), size=0.2) +
   scale_y_continuous(breaks=log(myrange), labels=myrange, limits=c(log(12), log(48))) +
   labs(x='', y='Port stop (hours)') +
   coord_flip() +
   theme_classic() +
   theme(plot.title=element_text(size=12, face='bold', colour='black', family='Helvetica'),
      axis.text=element_text(size=6, colour='black', family='Helvetica'),
      axis.title=element_text(size=8, colour='black', family='Helvetica'),
      axis.line=element_line(color='black'),
      axis.ticks=element_line(color='black'),
      legend.text=element_text(size=6, colour='black', family='Helvetica'),
      legend.title=element_text(size=8, colour='black', family='Helvetica'))

ggsave('plots/port_stop/flag_group_foreign.pdf', p, height=2.5, width=3,  useDingbats=FALSE)






#-------------------------
# gear type

# model
model <- lmer(duration_h_log ~ 0 + vessel_class + (1|port_iso3), data=data)


# get confidence intervals
nranpars <- length(getME(model, 'theta'))
nfixpars <- length(fixef(model))

c1 <- confint(model, method='boot', nsim=1000,
   parm=(nranpars+2):(nranpars+nfixpars+1), parallel='multicore', ncpus=2)


c1 <- as.data.frame(c1)
colnames(c1) <- c('lower', 'upper')
c1$mean <- fixef(model)
c1$flag <- rownames(c1)
c1 <- c1[c1$flag != 'vessel_classdriftnets',]
c1 <- c1[order(c1$mean, decreasing=FALSE),]
c1$flag <- factor(c1$flag, levels=c1$flag)

write.csv(c1, 'data/port_stop/gear_ci_foreign.csv')

c1 <- read.csv('data/port_stop/gear_ci_foreign.csv')
myrange <- seq(12, 48, 2)


p <- ggplot(aes(x=flag, y=mean), data=c1) +
   geom_pointrange(aes(ymin=lower, ymax=upper), size=0.2) +
   scale_y_continuous(breaks=log(myrange), labels=myrange, limits=c(log(12), log(48))) +
   labs(x='', y='Port stop (hours)') +
   coord_flip() +
   theme_classic() +
   theme(plot.title=element_text(size=12, face='bold', colour='black', family='Helvetica'),
      axis.text=element_text(size=6, colour='black', family='Helvetica'),
      axis.title=element_text(size=8, colour='black', family='Helvetica'),
      axis.line=element_line(color='black'),
      axis.ticks=element_line(color='black'),
      legend.text=element_text(size=6, colour='black', family='Helvetica'),
      legend.title=element_text(size=8, colour='black', family='Helvetica'))

ggsave('plots/port_stop/gear_foreign.pdf', p, height=2.5, width=3,  useDingbats=FALSE)

