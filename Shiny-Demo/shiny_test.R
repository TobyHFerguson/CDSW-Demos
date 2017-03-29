install.packages('shiny') 

library('sense')
library('shiny')
library('parallel')

mcparallel(runApp(host="0.0.0.0", port=8080, launch.browser=FALSE,
 appDir="/home/sense/app", display.mode="auto"))

service.url <- paste("http://", Sys.getenv("SENSE_DASHBOARD_ID"), ".",
Sys.getenv("SENSE_DOMAIN"), sep="")
Sys.sleep(5)

iframe(src=service.url, width="640px", height="480px")
