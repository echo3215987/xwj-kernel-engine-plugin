package com.foxconn.iisd.bd.rca

import java.io._
import java.net.URI

import com.foxconn.iisd.bd.rca.XWJKernelEnginePlugin.{configLoader, ctrlACode, ctrlCCode}
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
            test_item.split(ctrlACode).map(item => item.concat(ctrlCCode)).mkString(ctrlACode)
        }
    }
    //Catridge----end

    //取split最後一個element
    def getLast = udf((xs: Seq[String]) => (xs.last))

    //IPPD----start
    //取得測試樓層與線體對應表
    def getFloorLine = udf {
        s: String =>
            configLoader.getString("test_floor_line", "code_"+s)
    }
    //取得測試Spec的key
    def splitKey = udf {
        s: String =>
            s.trim.split("Name=\"").slice(1, s.length).map(_.split("\"")(0).trim)
    }
    //取得測試Spec的Value
    def splitValue = udf {
        s: String =>
            s.trim.split("Value=\"").slice(1, s.length).map(_.split("\"")(0).trim)
    }
    //取得測試上下界
    def getSpec = udf {
        (item: String, compare: Seq[String]) => item  match {
            case "PcaVerifyFirmwareRev\004ReadVersion" => {
                /*var upper = compare.filter(testparm=> testparm.contains("ValidFWRevs")).mkString.split("=")(1)
                var lower = upper
                Vector(upper, lower)*/
                getEqualSpec(compare, "ValidFWRevs")
            }
            case "PcaVerifyFirmwareRev\004ActualFWUpdate" => {
                getEqualSpec(compare, "FWUpdateStatus")
            }
            /*沒有對應測項值的spec
            case "PcaVerifyModelCode\004ResultInfo" => {
                var upper = compare.filter(testparm=> testparm.contains("ModelCode")).mkString.split("=")(1)
                var lower = upper
                Vector(upper, lower)
            }*/
            case "PcaBoardRevision\004ReadBoardRev" => {
                getEqualSpec(compare, "ExpectedBoardRev")
            }
            case "PcaSetDerivativeTrayIDNVM_Taiji.1\004UnitInfoSerialNumber" => {
                getEqualSpec(compare, "Serial_Number")
            }
            case "PcaSetDerivativeTrayIDNVM_Taiji.1\004DerivativeRead" => {
                getEqualSpec(compare, "Derivative")
            }
            case "Scan3PWM\004AvgPWMHomeMoveOut" | "Scan3PWM\004AvgPWMHomeMoveBack" => {
                getMaxMinSpec(compare, "SpecAvgPWMMax", "SpecAvgPWMMin")
            }
            case "Scan3PWM\004MaxPWMHomeMoveOut" | "Scan3PWM\004MaxPWMHomeMoveBack" => {
                getMaxMinSpec(compare, "SpecMaxPWMMax", "SpecMaxPWMMin")
            }
            //MechTestCarriage_WuDang非單一測項的spec
            case "MechTestCarriage_WuDang\004Maximum" => {
                getMaxSpec(compare, "MaxLimit")
            }
            //MechCalibratePaperMotor_WuDang非單一測項的spec

            //MechPWMTest_WuDang非單一測項的spec
            case "MechPWMTest_WuDang\004TOFtoOOPSlength" => {
                getMaxSpec(compare, "TOF2OopsLenMax")
            }

            //Scan3LEDInfo_SiriusFW
            //1.AFEGain<SpecAFEGain
            case "Scan3LEDInfo_SiriusFW\004AFEGain" => {
                getMaxSpec(compare, "SpecAFEGain")
            }
            //2.SpecCurrentMin_mA<LEDCurrentO<SpecCurrentOMax_mA
            case "Scan3LEDInfo_SiriusFW\004LEDCurrentO" => {
                getMaxMinSpec(compare, "SpecCurrentOMax_mA", "SpecCurrentMin_mA")
            }
            //3.SpecCurrentMin_mA<LEDCurrentG<SpecCurrentGMax_mA
            case "Scan3LEDInfo_SiriusFW\004LEDCurrentG" => {
                getMaxMinSpec(compare, "SpecCurrentGMax_mA", "SpecCurrentMin_mA")
            }
            //4.SpecCurrentMin_mA<LEDCurrentV<SpecCurrentVMax_mA
            case "Scan3LEDInfo_SiriusFW\004LEDCurrentV" => {
                getMaxMinSpec(compare, "SpecCurrentVMax_mA", "SpecCurrentMin_mA")
            }
            //5.SpecLevelGMin_Bits8<LevelAchievedG<SpecLevelGMax_Bits8
            case "Scan3LEDInfo_SiriusFW\004LevelAchievedG" => {
                getMaxMinSpec(compare, "SpecLevelGMax_Bits8", "SpecLevelGMin_Bits8")
            }
            //6.SpecLevelOMin_Bits8<LevelAchievedO<SpecLevelOMax_Bits8
            case "Scan3LEDInfo_SiriusFW\004LevelAchievedO" => {
                getMaxMinSpec(compare, "SpecLevelOMax_Bits8", "SpecLevelOMin_Bits8")
            }
            //7.SpecLevelVMin_Bits8<LevelAchievedV<SpecLevelVMax_Bits8
            case "Scan3LEDInfo_SiriusFW\004LevelAchievedV" => {
                getMaxMinSpec(compare, "SpecLevelVMax_Bits8", "SpecLevelVMin_Bits8")
            }
            //Scan3PRNU_SiriusFW
            //1.WhiteMaxR<SpecWhiteMax_Bits12
            case "Scan3PRNU_SiriusFW\004WhiteMaxR" => {
                getMaxSpec(compare, "SpecWhiteMax_Bits12")
            }
            //2.WhiteMaxG<SpecWhiteMax_Bits12
            case "Scan3PRNU_SiriusFW\004WhiteMaxG" => {
                getMaxSpec(compare, "SpecWhiteMax_Bits12")
            }
            //3.WhiteMaxB<SpecWhiteMax_Bits12
            case "Scan3PRNU_SiriusFW\004WhiteMaxB" => {
                getMaxSpec(compare, "SpecWhiteMax_Bits12")
            }
            //4.WhiteMinR>SpecWhiteMin_Bits12
            case "Scan3PRNU_SiriusFW\004WhiteMinR" => {
                getMinSpec(compare, "SpecWhiteMin_Bits12")
            }
            //5.WhiteMinG>SpecWhiteMin_Bits12
            case "Scan3PRNU_SiriusFW\004WhiteMinG" => {
                getMinSpec(compare, "SpecWhiteMin_Bits12")
            }
            //6.WhiteMinB>SpecWhiteMin_Bits12
            case "Scan3PRNU_SiriusFW\004WhiteMinB" => {
                getMinSpec(compare, "SpecWhiteMin_Bits12")
            }
            //7.WhiteMaxR-WhiteMinR<SpecWhiteMinOfMax_Bits12, 非單一測項的spec
            //8.WhiteMaxG-WhiteMinG<SpecWhiteMinOfMax_Bits12, 非單一測項的spec
            //9.WhiteMaxB-WhiteMinB<SpecWhiteMinOfMax_Bits12, 非單一測項的spec

            //10.DarkMaxRGB<SpecDarkMax_Bits12
            case "Scan3PRNU_SiriusFW\004DarkMaxRGB" => {
                getMaxSpec(compare, "SpecDarkMax_Bits12")
            }
            //11.DarkMinRGB>SpecDarkMin_Bits12
            case "Scan3PRNU_SiriusFW\004DarkMinRGB" => {
                getMinSpec(compare, "SpecDarkMin_Bits12")
            }
            //12.DarkDelta<SpecDarkDelta_Bits12
            case "Scan3PRNU_SiriusFW\004DarkDelta" => {
                getMaxSpec(compare, "SpecDarkDelta_Bits12")
            }
            //13.SpecMinDocOffsetX_Pix300<DocOffsetX<SpecMaxDocOffsetX_Pix300
            case "Scan3PRNU_SiriusFW\004DocOffsetX" => {
                getMaxMinSpec(compare, "SpecMaxDocOffsetX_Pix300", "SpecMinDocOffsetX_Pix300")
            }
            //14.SpecMinDocOffsetY_Pix300<DocOffsetY<SpecMaxDocOffsetY_Pix300
            case "Scan3PRNU_SiriusFW\004DocOffsetY" => {
                getMaxMinSpec(compare, "SpecMaxDocOffsetY_Quads", "SpecMinDocOffsetY_Quads")
            }
            //Scan3MOMS
            //1.SpecTopMarginErrorMin_mm<TopMarginError_mm<SpecTopMarginErrorMax_mm
            case "Scan3MOMS\004TopMarginError_mm" => {
                getMaxMinSpec(compare, "SpecTopMarginErrorMax_mm", "SpecTopMarginErrorMin_mm")
            }
            //2.SpecBotMarginErrorMin_mm<BotMarginError_mm<SpecBotMarginErrorMax_mm
            case "Scan3MOMS\004BotMarginError_mm" => {
                getMaxMinSpec(compare, "SpecBotMarginErrorMax_mm", "SpecBotMarginErrorMin_mm")
            }
            //3.SpecLftMarginErrorMin_mm<LftMarginErrorTop_mm<SpecLftMarginErrorMax_mm
            case "Scan3MOMS\004LftMarginErrorTop_mm" => {
                getMaxMinSpec(compare, "SpecLftMarginErrorMax_mm", "SpecLftMarginErrorMin_mm")
            }
            //4.SpecRgtMarginErrorMin_mm<RgtMarginErrorTop_mm<SpecRgtMarginErrorMax_mm
            case "Scan3MOMS\004RgtMarginErrorTop_mm" => {
                getMaxMinSpec(compare, "SpecRgtMarginErrorMax_mm", "SpecRgtMarginErrorMin_mm")
            }
            //5.SpecLftMarginErrorMin_mm<LftMarginErrorBot_mm<SpecLftMarginErrorMax_mm
            case "Scan3MOMS\004LftMarginErrorBot_mm" => {
                getMaxMinSpec(compare, "SpecLftMarginErrorMax_mm", "SpecLftMarginErrorMin_mm")
            }
            //6.SpecRgtMarginErrorMin_mm<RgtMarginErrorBot_mm<SpecRgtMarginErrorMax_mm
            case "Scan3MOMS\004RgtMarginErrorBot_mm" => {
                getMaxMinSpec(compare, "SpecRgtMarginErrorMax_mm", "SpecRgtMarginErrorMin_mm")
            }
            //7.CtrXMarginError1_mm<SpecCenterXError_mm
            case "Scan3MOMS\004CtrXMarginError1_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //8.CtrXMarginError2_mm<SpecCenterXError_mm
            case "Scan3MOMS\004CtrXMarginError2_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //9.CtrXMarginError3_mm<SpecCenterXError_mm
            case "Scan3MOMS\004CtrXMarginError3_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //10.CtrXMarginError4_mm<SpecCenterXError_mm
            case "Scan3MOMS\004CtrXMarginError4_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //11.CtrXMarginError5_mm<SpecCenterXError_mm
            case "Scan3MOMS\004CtrXMarginError5_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //12.CtrXMarginError6_mm<SpecCenterXError_mm
            case "Scan3MOMS\004CtrXMarginError6_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //13.TopSkewError_Deg<SpecTopSkewError_Deg
            case "Scan3MOMS\004TopSkewError_Deg" => {
                getMaxSpec(compare, "SpecTopSkewError_Deg")
            }
            //14.FeedSkewError_Deg<SpecFeedSkewError_Deg
            case "Scan3MOMS\004FeedSkewError_Deg" => {
                getMaxSpec(compare, "SpecFeedSkewError_Deg")
            }
            //15.SpecMagXErrorMin_Per<MagXError_Percent<SpecMagXErrorMax_Per
            case "Scan3MOMS\004MagXError_Percent" => {
                getMaxMinSpec(compare, "SpecMagXErrorMax_Per", "SpecMagXErrorMin_Per")
            }
            //16.SpecMagYErrorMin_Per<MagYError_Percent<SpecMagYErrorMax_Per
            case "Scan3MOMS\004MagYError_Percent" => {
                getMaxMinSpec(compare, "SpecMagYErrorMax_Per", "SpecMagYErrorMin_Per")
            }
            //17.OrthogError1_Deg<SpecOrthog_Deg
            case "Scan3MOMS\004OrthogError1_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //18.OrthogError2_Deg<SpecOrthog_Deg
            case "Scan3MOMS\004OrthogError2_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //19.OrthogError3_Deg<SpecOrthog_Deg
            case "Scan3MOMS\004OrthogError3_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //20.OrthogError4_Deg<SpecOrthog_Deg
            case "Scan3MOMS\004OrthogError4_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //21.OrthogError5_Deg<SpecOrthog_Deg
            case "Scan3MOMS\004OrthogError5_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //22.SpecMinDocOffsetX_Pix300<DocOffsetX<SpecMaxDocOffsetX_Pix300
            case "Scan3MOMS\004DocOffsetX" => {
                getMaxMinSpec(compare, "SpecMaxDocOffsetX_Pix300", "SpecMinDocOffsetX_Pix300")
            }
            //23.SpecMinDocOffsetY_Quads<DocOffsetY<SpecMaxDocOffsetY_Quads
            case "Scan3MOMS\004DocOffsetY" => {
                getMaxMinSpec(compare, "SpecMaxDocOffsetY_Quads", "SpecMinDocOffsetY_Quads")
            }
            //Scan3StreakRGB
            //1.LightStreakRed_MaxDelta_Zone1<SpecLightStreakinRed_Bits8
            case "Scan3StreakRGB\004LightStreakRed_MaxDelta_Zone1" => {
                getMaxSpec(compare, "SpecLightStreakinRed_Bits8")
            }
            //2.LightStreakGreen_MaxDelta_Zone1<SpecLightStreakinGreen_Bits8
            case "Scan3StreakRGB\004LightStreakGreen_MaxDelta_Zone1" => {
                getMaxSpec(compare, "SpecLightStreakinGreen_Bits8")
            }
            //3.LightStreakBlue_MaxDelta_Zone1<SpecLightStreakinBlue_Bits8
            case "Scan3StreakRGB\004LightStreakBlue_MaxDelta_Zone1" => {
                getMaxSpec(compare, "SpecLightStreakinBlue_Bits8")
            }
            //Scan3IL
            //1.MaxDelta_RedBar_Ch1<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_RedBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //2.MaxDelta_RedBar_Ch2<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_RedBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //3.MaxDelta_RedBar_Ch3<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_RedBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //4.MaxDelta_GreBar_Ch1<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_GreBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //5.MaxDelta_GreBar_Ch2<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_GreBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //6.MaxDelta_GreBar_Ch3<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_GreBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //7.MaxDelta_BluBar_Ch1<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_BluBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //8.MaxDelta_BluBar_Ch2<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_BluBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //9.MaxDelta_BluBar_Ch3<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxDelta_BluBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //10.MaxAvg_RedBar_Ch1<SpecRedNomCh123_Bits8.1+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_RedBar_Ch1" => {
                getMaxComputationSpec(compare, "SpecRedNomCh123_Bits8", 0, "SpecMaxChipTolFromNom_Bits8")
            }
            //11.MaxAvg_RedBar_Ch2<SpecRedNomCh123_Bits8.2+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_RedBar_Ch2" => {
                getMaxComputationSpec(compare, "SpecRedNomCh123_Bits8", 1, "SpecMaxChipTolFromNom_Bits8")
            }
            //12.MaxAvg_RedBar_Ch3<SpecRedNomCh123_Bits8.3+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_RedBar_Ch3" => {
                getMaxComputationSpec(compare, "SpecRedNomCh123_Bits8", 2,"SpecMaxChipTolFromNom_Bits8")
            }
            //13.MinAvg_RedBar_Ch1>SpecRedNomCh123_Bits8.1-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_RedBar_Ch1" => {
                getMinComputationSpec(compare, "SpecRedNomCh123_Bits8", 0, "SpecMaxChipTolFromNom_Bits8")
            }
            //14.MinAvg_RedBar_Ch2>SpecRedNomCh123_Bits8.2-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_RedBar_Ch2" => {
                getMinComputationSpec(compare, "SpecRedNomCh123_Bits8", 1, "SpecMaxChipTolFromNom_Bits8")
            }
            //15.MinAvg_RedBar_Ch3>SpecRedNomCh123_Bits8.3-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_RedBar_Ch3" => {
                getMinComputationSpec(compare, "SpecRedNomCh123_Bits8", 2,"SpecMaxChipTolFromNom_Bits8")
            }
            //16.MaxAvg_GreBar_Ch1<SpecGreenNomCh123_Bits8.1+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_GreBar_Ch1" => {
                getMaxComputationSpec(compare, "SpecGreenNomCh123_Bits8", 0,"SpecMaxChipTolFromNom_Bits8")
            }
            //17.MaxAvg_GreBar_Ch2<SpecGreenNomCh123_Bits8.2+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_GreBar_Ch2" => {
                getMaxComputationSpec(compare, "SpecGreenNomCh123_Bits8", 1, "SpecMaxChipTolFromNom_Bits8")
            }
            //18.MaxAvg_GreBar_Ch3<SpecGreenNomCh123_Bits8.3+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_GreBar_Ch3" => {
                getMaxComputationSpec(compare, "SpecGreenNomCh123_Bits8", 2, "SpecMaxChipTolFromNom_Bits8")
            }
            //19.MinAvg_GreBar_Ch1>SpecGreenNomCh123_Bits8.1-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_GreBar_Ch1" => {
                getMinComputationSpec(compare, "SpecGreenNomCh123_Bits8", 0, "SpecMaxChipTolFromNom_Bits8")
            }
            //20.MinAvg_GreBar_Ch2>SpecGreenNomCh123_Bits8.2-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_GreBar_Ch2" => {
                getMinComputationSpec(compare, "SpecGreenNomCh123_Bits8", 1, "SpecMaxChipTolFromNom_Bits8")
            }
            //21.MinAvg_GreBar_Ch3>SpecGreenNomCh123_Bits8.3-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_GreBar_Ch3" => {
                getMinComputationSpec(compare, "SpecGreenNomCh123_Bits8", 2, "SpecMaxChipTolFromNom_Bits8")
                /*var lowerValue = compare.filter(testparm=> testparm.contains("SpecGreenNomCh123_Bits8.3")).mkString.split("=")(1).toFloat
                var lowerSub = compare.filter(testparm=> testparm.contains("SpecMaxChipTolFromNom_Bits8")).mkString.split("=")(1).toFloat

                var upper = ""
                var lower = (lowerValue - lowerSub).toString
                Vector(upper, lower)*/
            }
            //22.MaxAvg_BluBar_Ch1<SpecBlueNomCh123_Bits8.1+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_BluBar_Ch1" => {
                getMaxComputationSpec(compare, "SpecBlueNomCh123_Bits8", 0, "SpecMaxChipTolFromNom_Bits8")
            }
            //23.MaxAvg_BluBar_Ch2<SpecBlueNomCh123_Bits8.2+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_BluBar_Ch2" => {
                getMaxComputationSpec(compare, "SpecBlueNomCh123_Bits8", 1, "SpecMaxChipTolFromNom_Bits8")
            }
            //24.MaxAvg_BluBar_Ch3<SpecBlueNomCh123_Bits8.3+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MaxAvg_BluBar_Ch3" => {
                getMaxComputationSpec(compare, "SpecBlueNomCh123_Bits8", 2, "SpecMaxChipTolFromNom_Bits8")
                /*
                var upper = compare.filter(testparm=> testparm.contains("SpecBlueNomCh123_Bits8.3") | testparm.contains("SpecMaxChipTolFromNom_Bits8"))
                  .map(_.split("=")(1).toFloat).sum.toString
                var lower = ""
                Vector(upper, lower)*/
            }
            //25.MinAvg_BluBar_Ch1>SpecBlueNomCh123_Bits8.1-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_BluBar_Ch1" => {
                getMinComputationSpec(compare, "SpecBlueNomCh123_Bits8", 0, "SpecMaxChipTolFromNom_Bits8")
            }
            //26.MinAvg_BluBar_Ch2>SpecBlueNomCh123_Bits8.2-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_BluBar_Ch2" => {
                getMinComputationSpec(compare, "SpecBlueNomCh123_Bits8", 1, "SpecMaxChipTolFromNom_Bits8")
            }
            //27.MinAvg_BluBar_Ch3>SpecBlueNomCh123_Bits8.3-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL\004MinAvg_BluBar_Ch3" => {
                getMinComputationSpec(compare, "SpecBlueNomCh123_Bits8", 2, "SpecMaxChipTolFromNom_Bits8")
            }
            //28.MaxStd_RedBar_Ch1<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_RedBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //29.MaxStd_RedBar_Ch2<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_RedBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //30.MaxStd_RedBar_Ch3<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_RedBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //31.MaxStd_GreBar_Ch1<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_GreBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //32.MaxStd_GreBar_Ch2<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_GreBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //33.MaxStd_GreBar_Ch3<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_GreBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //34.MaxStd_BluBar_Ch1<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_BluBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //35.MaxStd_BluBar_Ch2<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_BluBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //36.MaxStd_BluBar_Ch3<SpecMaxChipStd_Bits8
            case "Scan3IL\004MaxStd_BluBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //Scan3StreakFlare
            //1.LightStreakinBlack_MaxDelta_1<SpecLightStreakinBlack_Bits8
            case "Scan3StreakFlare\004LightStreakinBlack_MaxDelta_1" => {
                getMaxSpec(compare, "SpecLightStreakinBlack_Bits8")
            }
            //2.LightStreakinBlack_MaxDelta_2<SpecLightStreakinBlack_Bits8
            case "Scan3StreakFlare\004LightStreakinBlack_MaxDelta_2" => {
                getMaxSpec(compare, "SpecLightStreakinBlack_Bits8")
            }
            //3.LightStreakinWhite_MaxDelta<SpecLightStreakinWhite_Bits8
            case "Scan3StreakFlare\004LightStreakinWhite_MaxDelta" => {
                getMaxSpec(compare, "SpecLightStreakinWhite_Bits8")
            }
            //4.DarkStreakinBlack_MaxDelta<SpecDarkStreakinBlack_Bits8
            case "Scan3StreakFlare\004DarkStreakinBlack_MaxDelta" => {
                getMaxSpec(compare, "SpecDarkStreakinBlack_Bits8")
            }
            //5.DarkStreakinWhite_MaxDelta<SpecDarkStreakinWhite_Bits8
            case "Scan3StreakFlare\004DarkStreakinWhite_MaxDelta" => {
                getMaxSpec(compare, "SpecDarkStreakinWhite_Bits8")
            }
            //6.FlareStreak_MaxDelta<SpecFlareStreak_Bits8
            case "Scan3StreakFlare\004FlareStreak_MaxDelta" => {
                getMaxSpec(compare, "SpecFlareStreak_Bits8")
            }
            //Scan3SLI
            //1.MaxFullCurvature_Pix<SpecMaxFullCurvature_Pix600
            case "Scan3SLI\004MaxFullCurvature_Pix" => {
                getMaxSpec(compare, "SpecMaxFullCurvature_Pix600")
            }
            //2.MaxLocalCurvature_Pix<SpecMaxLocalCurvature_Pix600
            case "Scan3SLI\004MaxLocalCurvature_Pix" => {
                getMaxSpec(compare, "SpecMaxLocalCurvature_Pix600")
            }
            //3.MaxStep_Microns<SpecMaxStep_Microns
            case "Scan3SLI\004MaxStep_Microns" => {
                getMaxSpec(compare, "SpecMaxStep_Microns")
            }
            //4.MaxSlantChange_MicronsPerMM<SpecMaxSlantDelta_MicronsPerMM
            case "Scan3SLI\004MaxSlantChange_MicronsPerMM" => {
                getMaxSpec(compare, "SpecMaxSlantDelta_MicronsPerMM")
            }
            //Scan3MTF　
            //1.MinMTF>SpecMinMTF
            case "Scan3MTF\004MinMTF" => {
                getMinSpec(compare, "SpecMinMTF")
            }
            //2.MaxMTF<SpecMaxMTF
            case "Scan3MTF\004MaxMTF" => {
                getMaxSpec(compare, "SpecMaxMTF")
            }
            //Scan3BPRWPR.1
            //1.MaxAvgL<SpecMaxAvgL_Bits8
            case "Scan3BPRWPR.1\004MaxAvgL" => {
                getMaxSpec(compare, "SpecMaxAvgL_Bits8")
            }
            //2.MinAvgL>SpecMinAvgL_Bits8
            case "Scan3BPRWPR.1\004MinAvgL" => {
                getMinSpec(compare, "SpecMinAvgL_Bits8")
            }
            //3.MaxAvgC<SpecMaxAvgC_Bits8
            case "Scan3BPRWPR.1\004MaxAvgC" => {
                getMaxSpec(compare, "SpecMaxAvgC_Bits8")
            }
            //4.MaxDeltaL<SpecMaxDeltaL_Bits8
            case "Scan3BPRWPR.1\004MaxDeltaL" => {
                getMaxSpec(compare, "SpecMaxDeltaL_Bits8")
            }
            //5.MaxDeltaAB<SpecMaxDeltaAB_Bits8
            case "Scan3BPRWPR.1\004MaxDeltaAB" => {
                getMaxSpec(compare, "SpecMaxDeltaAB_Bits8")
            }
            //6.MaxNoiseL<SpecMaxNoiseL_Bits8
            case "Scan3BPRWPR.1\004MaxNoiseL" => {
                getMaxSpec(compare, "SpecMaxNoiseL_Bits8")
            }
            //7.MaxNoiseA<SpecMaxNoiseAB_Bits8
            case "Scan3BPRWPR.1\004MaxNoiseA" => {
                getMaxSpec(compare, "SpecMaxNoiseAB_Bits8")
            }
            //8.MaxNoiseB<SpecMaxNoiseAB_Bits8
            case "Scan3BPRWPR.1\004MaxNoiseB" => {
                getMaxSpec(compare, "SpecMaxNoiseAB_Bits8")
            }
            //9.MaxRowNoiseL<SpecMaxRowNoiseL_Bits8
            case "Scan3BPRWPR.1\004MaxRowNoiseL" => {
                getMaxSpec(compare, "SpecMaxRowNoiseL_Bits8")
            }
            //10.MaxRowNoiseA<SpecMaxRowNoiseAB_Bits8
            case "Scan3BPRWPR.1\004MaxRowNoiseA" => {
                getMaxSpec(compare, "SpecMaxRowNoiseAB_Bits8")
            }
            //11.MaxRowNoiseB<SpecMaxRowNoiseAB_Bits8
            case "Scan3BPRWPR.1\004MaxRowNoiseB" => {
                getMaxSpec(compare, "SpecMaxRowNoiseAB_Bits8")
            }
            //12.ColNoiseStd<SpecMaxColStd_Bits8
            case "Scan3BPRWPR.1\004ColNoiseStd" => {
                getMaxSpec(compare, "SpecMaxColStd_Bits8")
            }
            //Scan3BPRWPR.2
            //1.MaxAvgL<SpecMaxAvgL_Bits8
            case "Scan3BPRWPR.2\004MaxAvgL" => {
                getMaxSpec(compare, "SpecMaxAvgL_Bits8")
            }
            //2.MinAvgL>SpecMinAvgL_Bits8
            case "Scan3BPRWPR.2\004MinAvgL" => {
                getMinSpec(compare, "SpecMinAvgL_Bits8")
            }
            //3.MaxAvgC<SpecMaxAvgC_Bits8
            case "Scan3BPRWPR.2\004MaxAvgC" => {
                getMaxSpec(compare, "SpecMaxAvgC_Bits8")
            }
            //4.MaxDeltaL<SpecMaxDeltaL_Bits8
            case "Scan3BPRWPR.2\004MaxDeltaL" => {
                getMaxSpec(compare, "SpecMaxDeltaL_Bits8")
            }
            //5.MaxDeltaAB<SpecMaxDeltaAB_Bits8
            case "Scan3BPRWPR.2\004MaxDeltaAB" => {
                getMaxSpec(compare, "SpecMaxDeltaAB_Bits8")
            }
            //6.MaxNoiseL<SpecMaxNoiseL_Bits8
            case "Scan3BPRWPR.2\004MaxNoiseL" => {
                getMaxSpec(compare, "SpecMaxNoiseL_Bits8")
            }
            //7.MaxNoiseA<SpecMaxNoiseAB_Bits8
            case "Scan3BPRWPR.2\004MaxNoiseA" => {
                getMaxSpec(compare, "SpecMaxNoiseAB_Bits8")
            }
            //8.MaxNoiseB<SpecMaxNoiseAB_Bits8
            case "Scan3BPRWPR.2\004MaxNoiseB" => {
                getMaxSpec(compare, "SpecMaxNoiseAB_Bits8")
            }

            case "PcaVerifyCustomerDefaults_Taiji\004ActualFWUpdateDSID" | "MechPowerOffUUT_WuDang\004ActualFWUpdateDSID" => {
                getEqualSpec(compare, "FWUpdateDSID")
            }

            case _ => Vector("", "")
        }

    }

    def getSpecBoundary(compare: Seq[String], spec: String): String = {
        compare.filter(testparm => testparm.contains(spec+"=")).mkString.split("=")(1)
    }
    //上下界相等
    def getEqualSpec(compare: Seq[String], spec: String): Vector[String] = {
        var upper = getSpecBoundary(compare, spec)
        var lower = upper
        Vector(upper, lower)
    }
    //只有上界
    def getMaxSpec(compare: Seq[String], spec: String): Vector[String] = {
        var upper = getSpecBoundary(compare, spec)
        var lower = ""
        Vector(upper, lower)
    }
    //只有下界
    def getMinSpec(compare: Seq[String], spec: String): Vector[String] = {
        var upper = ""
        var lower = getSpecBoundary(compare, spec)
        Vector(upper, lower)
    }
    //上下界
    def getMaxMinSpec(compare: Seq[String], specMax: String, specMin: String): Vector[String] = {
        var upper = getSpecBoundary(compare, specMax)
        var lower = getSpecBoundary(compare, specMin)
        Vector(upper, lower)
    }
    //上界特別計算
    def getMaxComputationSpec(compare: Seq[String], specMax1: String, idx: Int, specMax2: String): Vector[String] = {
        var upperfirst = compare.filter(testparm=> testparm.contains(specMax1)).mkString.split("=")(1).split(";")(idx).toFloat
        var upperSecond = compare.filter(testparm=> testparm.contains(specMax2)).mkString.split("=")(1).toFloat
        var upper = (upperfirst + upperSecond).toString

        /*var upper = compare.filter(testparm=> testparm.contains(specMax1) | testparm.contains(specMax2))
          .map(_.split("=")(1).toFloat).sum.toString*/
        var lower = ""
        Vector(upper, lower)
    }
    //下界特別計算
    def getMinComputationSpec(compare: Seq[String], specMinValue: String, idx: Int, specMinSub: String): Vector[String] = {
        var lowerValue = compare.filter(testparm=> testparm.contains(specMinValue)).mkString.split("=")(1).split(";")(idx).toFloat
        var lowerSub = compare.filter(testparm=> testparm.contains(specMinSub)).mkString.split("=")(1).toFloat

        var upper = ""
        var lower = (lowerValue - lowerSub).toString
        Vector(upper, lower)
    }
    //IPPD----end
}
