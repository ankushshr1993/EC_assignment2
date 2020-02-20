setwd("~/Desktop/MyDocs/ec19-assignment2-master-96e949e857eb82b93b8f52f02eab127060d50236")
results313=read.csv('1results313.csv',sep=",")
results322=read.csv('1results322.csv',sep=",")
results331=read.csv('1results331.csv',sep=",")
library(ggplot2)



plots(results313)
plots(results322)
plots(results331)

statsdat<-function(dat){
  print("qwrite")
  datget = dat[which(dat$type=='qwrite'),]
  datget1 = datget[which(datget$status=='true'),]
  print(paste0("Failed:",nrow(datget)-nrow(datget1)))
  print(paste0("Min:",min(datget1$time *1000)))
  print(paste0("Max:",max(datget1$time *1000)))
  print(paste0("Avg:",mean(datget1$time *1000)))
  print("qread")
  datread = dat[which(dat$type=='qread'),]
  datread1 = datread[which(datread$status=='true'),]
  print(paste0("Failed:",nrow(datread)-nrow(datread1)))
  print(paste0("Min:",min(datread1$time *1000)))
  print(paste0("Max:",max(datread1$time *1000)))
  print(paste0("Avg:",mean(datread1$time *1000)))
}
lt <- data.frame(yint = c(5, 1221, 23.9), grp  = factor(c(1, 2, 3),
                               levels = 1:3,
                               labels = c("Min", "Max", "Avg")))
lt1 <- data.frame(yint = c(2, 23, 4.3), grp  = factor(c(1, 2, 3),
                                                        levels = 1:3,
                                                        labels = c("Min", "Max", "Avg")))

plots<-function(dat){
  datwrite = dat[which(dat$type=='qwrite'),]
  datwrite$id=1:nrow(datwrite)
  a <- ggplot(datwrite, aes(x = id, y = time*1000))
  a <- a+geom_line()+geom_point(aes(colour=status))+geom_hline(data = lt,
                                mapping = aes(yintercept = yint, linetype = grp, color = grp))
  a <-a+ggtitle("qwrite")
  print(a)
  datread = dat[which(dat$type=='qread'),]
  datread$id=1:nrow(datread)
  a <- ggplot(datread, aes(x = id, y = time*1000))
  a <- a+geom_line()+geom_point(aes(colour=status))+geom_hline(data = lt1,
                                                               mapping = aes(yintercept = yint, linetype = grp, color = grp))
  a <-a+ggtitle("qread")
  print(a)
}


          