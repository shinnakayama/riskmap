library(ggplot2)
library(gridExtra)

# SHAP importance (output of 'at_sea_analysis.py')
data <- read.csv('fishing_iuu_importance.csv')


# add variable names
data$var_name <- NA
for(i in 1:nrow(data)) {

   foo <- as.character(data$X[i])
   foo <- gsub('\\)', '', foo)
   foo <- gsub('\\(', '', foo)

   bar <- c()
   for(j in 1:2) {
      a <- trimws(strsplit(foo, ',')[[1]][j])
      a <- substr(a, 2, nchar(a)-1)
      a <- paste0(toupper(substr(a,1,1)), substr(a,2,nchar(a)))
      if(a=='Tas')      a <- 'Time at sea'
      if(a=='Gear')   a <- 'Gear type'
      bar[j] <- a
   }

   if(bar[1]==bar[2]) {
      data$var_name[i] <- bar[1]
   }
   if(bar[1]!=bar[2]) {
      data$var_name[i] <- paste(bar[1], bar[2], sep=bquote(' \u00D7\n'))
   }
}


# order
data <- data[order(data$mean),]
data$var_name <- factor(data$var_name, levels=data$var_name)


ggplot(aes(x=mean, y=var_name), data=data) +
   geom_col(width=0.6) +
   scale_x_continuous(expand=c(0,0), sec.axis = dup_axis(), limits=c(0, 0.38)) +
   labs(title='A', x='Importance', y='') +
   theme_classic() +
   theme(plot.title=element_text(hjust=0, face=2, size=8),
      axis.text = element_text(size=6, colour='black'),
      axis.title = element_text(size=8, colour='black'),
      axis.line=element_line(color='black'),
      axis.ticks=element_line(color='black'))


#--------------------
# effect (output of 'at_sea_analysis.py')
#--------------------

data <- read.csv('fishing_iuu_effect.csv')
data <- data[!is.na(data$mean),]


combo_idx <- c()
for(i in 1:nrow(data))
   if(grepl('\\(', as.character(data$X[i]))) combo_idx <- c(combo_idx, i)

solo <- data[-combo_idx,]
combo <- data[combo_idx,]


# clean up variable names
tas <- c('< 1 months','1-3 months', '3-6 months', '6-12 months', '> 12 months')
gear <- c('Set longline', 'Drifting longline', 'Purse seine', 'Squid jigger',
   'Pole and line', 'Trawler', 'Troller', 'Pots and traps', 'Set gillnet', 'Driftnet')
flag <- c('China', 'Flag group 1', 'Flag group 2', 'Flag group 3', 'Other')


solo$var1 <- NA
for(i in 1:nrow(solo)) {

   a <- as.character(solo$X[i])
   a <- strsplit(a, '_')[[1]]
   a <- paste(a, collapse=' ')

   if(a=='flag group other')                 a <- 'Other'
   if(a=='flag group group1')                a <- 'Flag group 1'
   if(a=='flag group group2')                a <- 'Flag group 2'
   if(a=='flag group group3')                a <- 'Flag group 3'
   if(a=='flag group china')                 a <- 'China'

   if(a=='vessel class set longline')        a <- 'Set longline'
   if(a=='vessel class drifting longline')   a <- 'Drifting longline'
   if(a=='vessel class purse seine')         a <- 'Purse seine'
   if(a=='vessel class squid jigger')        a <- 'Squid jigger'
   if(a=='vessel class set gillnet')         a <- 'Set gillnet'
   if(a=='vessel class pots and traps')      a <- 'Pots and traps'
   if(a=='vessel class trawlers')            a <- 'Trawler'
   if(a=='vessel class trollers')            a <- 'Troller'
   if(a=='vessel class driftnets')           a <- 'Driftnet'
   if(a=='vessel class pole and line')       a <- 'Pole and line'

   if(a=='time at sea 1 3m')                 a <- '1-3 months'
   if(a=='time at sea 3 6m')                 a <- '3-6 months'
   if(a=='time at sea 6 12m')                a <- '6-12 months'
   if(a=='time at sea less than 1m')         a <- '< 1 month'
   if(a=='time at sea 12m and more')         a <- '> 12 months'

   solo$var1[i] <- a

}


# remove tas
solo <- solo[solo$var1 %in% c(flag, gear),]
solo$class <- NA
solo$class[solo$var1 %in% gear] <- 'gear'
solo$class[solo$var1 %in% flag] <- 'flag'
solo$class <- factor(solo$class, levels=c('gear', 'flag'))
solo <- solo[order(solo$class, solo$mean),]
solo$var1 <- factor(solo$var1, levels=solo$var1)


# combo
combo$var1 <- NA
combo$var2 <- NA
for(i in 1:nrow(combo)) {

   foo <- as.character(combo$X[i])
   foo <- gsub('\\)', '', foo)
   foo <- gsub('\\(', '', foo)

   bar <- c()
   for(j in 1:2) {
      a <- trimws(strsplit(foo, ',')[[1]][j])
      a <- strsplit(a, '_')[[1]]
      a <- paste(a, collapse=' ')
      a <- substr(a, 2, nchar(a)-1)

      if(a=='flag group other')                 a <- 'Other'
      if(a=='flag group group1')                a <- 'Flag group 1'
      if(a=='flag group group2')                a <- 'Flag group 2'
      if(a=='flag group group3')                a <- 'Flag group 3'
      if(a=='flag group china')                 a <- 'China'

      if(a=='vessel class set longline')        a <- 'Set longline'
      if(a=='vessel class drifting longline')   a <- 'Drifting longline'
      if(a=='vessel class purse seine')         a <- 'Purse seine'
      if(a=='vessel class squid jigger')        a <- 'Squid jigger'
      if(a=='vessel class set gillnet')         a <- 'Set gillnet'
      if(a=='vessel class pots and traps')      a <- 'Pots and traps'
      if(a=='vessel class trawlers')            a <- 'Trawler'
      if(a=='vessel class trollers')            a <- 'Troller'
      if(a=='vessel class driftnets')           a <- 'Driftnet'
      if(a=='vessel class pole and line')       a <- 'Pole and line'

      if(a=='time at sea 1 3m')                 a <- '1-3 months'
      if(a=='time at sea 3 6m')                 a <- '3-6 months'
      if(a=='time at sea 6 12m')                a <- '6-12 months'
      if(a=='time at sea less than 1m')         a <- '< 1 month'
      if(a=='time at sea 12m and more')         a <- '> 12 months'

      bar[j] <- a
   }
   if(bar[1] %in% flag) {
      bar <- bar[c(2,1)]
   }

   combo$var1[i] <- bar[1]
   combo$var2[i] <- bar[2]

}

combo <- combo[combo$var1 %in% c(flag, gear),]

idx <- c()
for(i in 1:nrow(combo)) {
   if(combo$var2[i] %in% flag) idx <- c(idx, i)
}

combo <- combo[idx,]
subsolo <- solo[which(solo$var1 %in% flag),1:7]
subsolo$var2 <- NA
combo <- rbind(combo, subsolo)

combo$class <- NA
combo$class[combo$var1 %in% gear] <- 'gear'
combo$class[combo$var1 %in% flag] <- 'flag'
combo$class <- as.factor(combo$class)
combo <- combo[order(combo$class, combo$mean),]
combo$var1 <- factor(combo$var1, levels=levels(solo$var1))


solo$var2 <- NA
mycol <- c('#603e95', '#d7255d', '#fac22b', '#009da1', '#cccccc')
combo$var2 <- factor(combo$var2, levels=c('Flag group 1','Flag group 2','Flag group 3', 'China','Other'))

ggplot(aes(x=mean, y=var1, colour=var2), data=combo) +
   geom_errorbarh(aes(xmin=lower, xmax=upper), height=0, size=0.3, position=position_dodge(width=0.1)) +
   geom_point(size=0.8,position=position_dodge(width=0.1)) +
   scale_colour_discrete(na.translate=FALSE, name='Flag') +
   geom_errorbarh(aes(xmin=lower, xmax=upper), colour='black', data=solo, height=0, size=0.3) +
   geom_point(aes(x=mean, y=var1), size=1.5, colour='black', data=solo) +
   geom_hline(yintercept=9.5, linetype=2, colour='grey', size=0.3) +
   scale_x_continuous(expand=c(0,0), sec.axis = dup_axis(), limits=c(-2, 2)) +
   labs(title='A', x='Port risk score', y='') +
   annotate('text', x=c(-1.9, -1.9), y=c(8.9,13.9), label=c('Gear type', 'Flag'),
      fontface=3, hjust=0, vjust=0, size=2) +
   theme_classic() +
   theme(plot.title=element_text(hjust=0, face=2, size=8),
      axis.text=element_text(size=6, colour='black'),
      axis.title=element_text(size=8, colour='black'),
      legend.title=element_text(size=8),
      legend.text=element_text(size=6),
      legend.key.size=unit(0.3,'cm'),
      axis.line=element_line(color='black'),
      axis.ticks=element_line(color='black'))
