package com.ftchinese.jobs.database

import org.springframework.context.annotation.{Bean, Configuration}

/**
 * Bean configure
 * Created by GWB on 2014/12/11.
 */
@Configuration
class BeanConfig {
    @Bean
    def AnalyticDataSource(): AnalyticDataSource = {new AnalyticDataSource()}
    @Bean
    def AnalyticDB(): AnalyticDB = {new AnalyticDB()}
}
