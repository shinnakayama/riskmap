library(ggplot2)
library(scales)
library(data.table)
library(maps)
library(colorRamps)
library(gridExtra)
library(reshape2)


# weighted KDE from package ggturn
kde2d.weighted <- function (x, y, w, h, n = n, lims = c(range(x), range(y))) {
  nx <- length(x)
  if (length(y) != nx)
      stop("data vectors must be the same length")
  gx <- seq(lims[1], lims[2], length = n) # gridpoints x
  gy <- seq(lims[3], lims[4], length = n) # gridpoints y
  if (missing(h))
    h <- c(bw(x), bw(y));
  if (missing(w))
    w <- numeric(nx)+1;
  h <- h/4
  ax <- outer(gx, x, "-")/h[1] # distance of each point to each grid point in x-direction
  ay <- outer(gy, y, "-")/h[2] # distance of each point to each grid point in y-direction
  z <- (matrix(rep(w,n), nrow=n, ncol=nx, byrow=TRUE)*matrix(dnorm(ax), n, nx)) %*% t(matrix(dnorm(ay), n, nx))/(sum(w) * h[1] * h[2]) # z is the density
  return(list(x = gx, y = gy, z = z))
}


# world map
world <- map_data('world')
p <- ggplot(world, aes(long, lat)) +
    coord_equal() +
    scale_x_continuous(limits=c(-180, 180), expand=c(0,0)) +
    scale_y_continuous(limits=c(-90, 90), expand=c(0,0)) +
    theme(plot.title=element_text(hjust=0.5),
    panel.grid.major=element_blank(),
    panel.grid.minor=element_blank(),
    axis.ticks=element_blank(),
    axis.text=element_blank(),
    axis.title=element_blank(),
    panel.background=element_rect(colour=NA, fill=NA))


#-------------------------
# IUU fishing
#-------------------------
# Import data (output of fishing_bin_iuu.py)
df <- fread('fishing_bin_iuu.csv')
df$fishing_hours_km2 <- df$fishing_hours/df$km2

# Low risk class
foo <- df[df$risk_class==0,]
min_val <- min(foo$fishing_hours_km2)
max_val <- max(foo$fishing_hours_km2)
foo$fishing_hours_km2_scaled <- (foo$fishing_hours_km2 - min_val)/(max_val - min_val)
bar <- kde2d.weighted(x=foo$lon_bin, y=foo$lat_bin,
   lims=c(-180,180,-90,90), n=361, h=c(5,5), w=foo$fishing_hours_km2_scaled)
grids <- expand.grid(lon=bar$x, lat=bar$y)
zz <- melt(bar$z)
zz$lon <- grids$lon
zz$lat <- grids$lat
qq <- quantile(zz$value, 0.95)


p + stat_contour(geom='polygon', aes(x=lon, y=lat, z=value, fill=factor(..level..)), data=zz, breaks=qq) +
   scale_fill_manual(values=alpha('#33b6ff', 0.5), labels=NULL, name= NULL) +
   geom_map(map=world, aes(map_id=region), color='#5e5e5e', fill='#5e5e5e', size=0) +
   guides(fill=guide_legend(keywidth=0.5, keyheight=0.5)) +
   labs(title='A') +
   theme(plot.title=element_text(hjust=0, face=2, size=8),
      legend.title=element_text(size=8),
      legend.text=element_text(size=6),
      legend.key=element_blank())


# medium risk class
foo <- df[df$risk_class==1,]
min_val <- min(foo$fishing_hours_km2)
max_val <- max(foo$fishing_hours_km2)
foo$fishing_hours_km2_scaled <- (foo$fishing_hours_km2 - min_val)/(max_val - min_val)
bar <- kde2d.weighted(x=foo$lon_bin, y=foo$lat_bin,
   lims=c(-180,180,-90,90), n=361, h=c(5,5), w=foo$fishing_hours_km2_scaled)
grids <- expand.grid(lon=bar$x, lat=bar$y)
zz <- melt(bar$z)
zz$lon <- grids$lon
zz$lat <- grids$lat
qq <- quantile(zz$value, 0.95)


p + stat_contour(geom='polygon', aes(x=lon, y=lat, z=value, fill=factor(..level..)), data=zz, breaks=qq) +
   scale_fill_manual(values=alpha('#ffda24', 0.5), labels=NULL, name=NULL) +
   geom_map(map=world, aes(map_id=region), color='#5e5e5e', fill='#5e5e5e', size=0) +
   guides(fill=guide_legend(keywidth=0.5, keyheight=0.5)) +
   labs(title='C') +
   theme(plot.title=element_text(hjust=0, face=2, size=8),
      legend.title=element_text(size=8),
      legend.text=element_text(size=6),
      legend.key=element_blank())


# high risk class
foo <- df[df$risk_class==2,]
min_val <- min(foo$fishing_hours_km2)
max_val <- max(foo$fishing_hours_km2)
foo$fishing_hours_km2_scaled <- (foo$fishing_hours_km2 - min_val)/(max_val - min_val)
bar <- kde2d.weighted(x=foo$lon_bin, y=foo$lat_bin,
   lims=c(-180,180,-90,90), n=361, h=c(5,5), w=foo$fishing_hours_km2_scaled)
grids <- expand.grid(lon=bar$x, lat=bar$y)
zz <- melt(bar$z)
zz$lon <- grids$lon
zz$lat <- grids$lat
qq <- quantile(zz$value, 0.95)


p + stat_contour(geom='polygon', aes(x=lon, y=lat, z=value, fill=factor(..level..)),
      data=zz, breaks=qq) +
   scale_fill_manual(values=alpha('#ff3e6c', 0.5), labels=NULL, name=NULL) +
   geom_map(map=world, aes(map_id=region), color='#5e5e5e', fill='#5e5e5e', size=0) +
   guides(fill=guide_legend(keywidth=0.5, keyheight=0.5)) +
   labs(title='E') +
   theme(plot.title=element_text(hjust=0, face=2, size=8),
      legend.title=element_text(size=8),
      legend.text=element_text(size=6),
      legend.key=element_blank())


#-------------------------
# Labor abuse
#-------------------------
# Import data (output of fishing_bin_la.py)
df <- fread('fishing_bin_la.csv')
df$fishing_hours_km2 <- df$fishing_hours/df$km2


# low risk class
foo <- df[df$risk_class==0,]
min_val <- min(foo$fishing_hours_km2)
max_val <- max(foo$fishing_hours_km2)
foo$fishing_hours_km2_scaled <- (foo$fishing_hours_km2 - min_val)/(max_val - min_val)
bar <- kde2d.weighted(x=foo$lon_bin, y=foo$lat_bin,
   lims=c(-180,180,-90,90), n=361, h=c(5,5), w=foo$fishing_hours_km2_scaled)
grids <- expand.grid(lon=bar$x, lat=bar$y)
zz <- melt(bar$z)
zz$lon <- grids$lon
zz$lat <- grids$lat
qq <- quantile(zz$value, 0.95)


p + stat_contour(geom='polygon', aes(x=lon, y=lat, z=value, fill=factor(..level..)),
      data=zz, breaks=qq) +
   scale_fill_manual(values=alpha('#33b6ff', 0.5), labels=NULL, name=NULL) +
   geom_map(map=world, aes(map_id=region), color='#5e5e5e', fill='#5e5e5e', size=0) +
   guides(fill=guide_legend(keywidth=0.5, keyheight=0.5)) +
   labs(title='B') +
   theme(plot.title=element_text(hjust=0, face=2, size=8),
      legend.title=element_text(size=8),
      legend.text=element_text(size=6),
      legend.key=element_blank())


# medium risk class
foo <- df[df$risk_class==1,]
min_val <- min(foo$fishing_hours_km2)
max_val <- max(foo$fishing_hours_km2)
foo$fishing_hours_km2_scaled <- (foo$fishing_hours_km2 - min_val)/(max_val - min_val)
bar <- kde2d.weighted(x=foo$lon_bin, y=foo$lat_bin,
   lims=c(-180,180,-90,90), n=361, h=c(5,5), w=foo$fishing_hours_km2_scaled)
grids <- expand.grid(lon=bar$x, lat=bar$y)
zz <- melt(bar$z)
zz$lon <- grids$lon
zz$lat <- grids$lat
qq <- quantile(zz$value, 0.95)


p + stat_contour(geom='polygon', aes(x=lon, y=lat, z=value, fill=factor(..level..)), data=zz, breaks=qq) +
   scale_fill_manual(values=alpha('#ffda24', 0.5), labels=NULL, name=NULL) +
   geom_map(map=world, aes(map_id=region), color='#5e5e5e', fill='#5e5e5e', size=0) +
   guides(fill=guide_legend(keywidth=0.5, keyheight=0.5)) +
   labs(title='D') +
   theme(plot.title=element_text(hjust=0, face=2, size=8),
      legend.title=element_text(size=8),
      legend.text=element_text(size=6),
      legend.key=element_blank())


# high risk class
foo <- df[df$risk_class==2,]
min_val <- min(foo$fishing_hours_km2)
max_val <- max(foo$fishing_hours_km2)
foo$fishing_hours_km2_scaled <- (foo$fishing_hours_km2 - min_val)/(max_val - min_val)
bar <- kde2d.weighted(x=foo$lon_bin, y=foo$lat_bin,
   lims=c(-180,180,-90,90), n=361, h=c(5,5), w=foo$fishing_hours_km2_scaled)
grids <- expand.grid(lon=bar$x, lat=bar$y)
zz <- melt(bar$z)
zz$lon <- grids$lon
zz$lat <- grids$lat
qq <- quantile(zz$value, 0.95)


p + stat_contour(geom='polygon', aes(x=lon, y=lat, z=value, fill=factor(..level..)), data=zz, breaks=qq) +
   scale_fill_manual(values=alpha('#ff3e6c', 0.5), labels=NULL, name=NULL) +
   geom_map(map=world, aes(map_id=region), color='#5e5e5e', fill='#5e5e5e', size=0) +
   guides(fill=guide_legend(keywidth=0.5, keyheight=0.5)) +
   labs(title='F') +
   theme(plot.title=element_text(hjust=0, face=2, size=8),
      legend.title=element_text(size=8),
      legend.text=element_text(size=6),
      legend.key=element_blank())
