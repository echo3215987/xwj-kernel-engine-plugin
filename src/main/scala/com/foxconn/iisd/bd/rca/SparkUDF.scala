package com.foxconn.iisd.bd.rca

import java.io._
import java.net.URI

import com.foxconn.iisd.bd.rca.XWJCartridgePlugin.{configLoader}
import org.apache.spark.sql.functions.{col, udf, when}

import scala.collection.mutable.{Seq, WrappedArray}

object SparkUDF {
    //Catridge----start
    def replaceTestResult = udf {
        result: Int =>{
            var resultValue = "fail"
            if(result == 0)
                resultValue = "pass"
            resultValue
        }
    }

    def replaceTestResultDetail = udf {
        result: String =>{
            var resultValue = result
            if(result.equals("0"))
                resultValue = "pass"
            resultValue
        }
    }

    def genTestItemSpec = udf {
        (test_item: String) =>{
            test_item.split(XWJKEPluginConstants.ctrlACode)
              .map(item => item.concat(XWJKEPluginConstants.ctrlCCode)).mkString(XWJKEPluginConstants.ctrlACode)
        }
    }
    //Catridge----end

    //取split最後一個element
    def getLast = udf((xs: Seq[String]) => (xs.last))
}
