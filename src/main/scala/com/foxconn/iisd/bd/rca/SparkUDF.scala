package com.foxconn.iisd.bd.rca

import java.io._
import java.net.URI

import com.foxconn.iisd.bd.rca.XWJKernelEnginePlugin.configLoader
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.{Seq, WrappedArray}

object SparkUDF {

    //取split最後一個element
    def getLast = udf((xs: Seq[String]) => (xs.last))
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
            case "PcaVerifyFirmwareRev^DReadVersion" => {
                /*var upper = compare.filter(testparm=> testparm.contains("ValidFWRevs")).mkString.split("=")(1)
                var lower = upper
                Vector(upper, lower)*/
                getEqualSpec(compare, "ValidFWRevs")
            }
            case "PcaVerifyFirmwareRev^DActualFWUpdate" => {
                getEqualSpec(compare, "FWUpdateStatus")
            }
            /*沒有對應測項值的spec
            case "PcaVerifyModelCode^DResultInfo" => {
                var upper = compare.filter(testparm=> testparm.contains("ModelCode")).mkString.split("=")(1)
                var lower = upper
                Vector(upper, lower)
            }*/
            case "PcaBoardRevision^DReadBoardRev" => {
                getEqualSpec(compare, "ExpectedBoardRev")
            }
            case "caSetDerivativeTrayIDNVM_Taiji.1^DUnitInfoSerialNumber" => {
                getEqualSpec(compare, "Serial_Number")
            }
            case "caSetDerivativeTrayIDNVM_Taiji.1^DDerivativeRead" => {
                getEqualSpec(compare, "Derivative")
            }
            case "Scan3PWM^DAvgPWMHomeMoveOut" | "Scan3PWM^DAvgPWMHomeMoveBack" => {
                getMaxMinSpec(compare, "SpecAvgPWMMax", "SpecAvgPWMMin")
            }
            case "Scan3PWM^DMaxPWMHomeMoveOut" | "Scan3PWM^DMaxPWMHomeMoveBack" => {
                getMaxMinSpec(compare, "SpecMaxPWMMax", "SpecMaxPWMMin")
            }
            //MechTestCarriage_WuDang非單一測項的spec
            case "MechTestCarriage_WuDang^DMaximum" => {
                getMaxSpec(compare, "MaxLimit")
            }
            //MechCalibratePaperMotor_WuDang非單一測項的spec

            //MechPWMTest_WuDang非單一測項的spec
            case "MechPWMTest_WuDang^DTOFtoOOPSlength" => {
                getMaxSpec(compare, "TOF2OopsLenMax")
            }

            //Scan3LEDInfo_SiriusFW
            //1.AFEGain<SpecAFEGain
            case "Scan3LEDInfo_SiriusFW^DAFEGain" => {
                getMaxSpec(compare, "SpecAFEGain")
            }
            //2.SpecCurrentMin_mA<LEDCurrentO<SpecCurrentOMax_mA
            case "Scan3LEDInfo_SiriusFW^DLEDCurrentO" => {
                getMaxMinSpec(compare, "SpecCurrentOMax_mA", "SpecCurrentMin_mA")
            }
            //3.SpecCurrentMin_mA<LEDCurrentG<SpecCurrentGMax_mA
            case "Scan3LEDInfo_SiriusFW^DLEDCurrentG" => {
                getMaxMinSpec(compare, "SpecCurrentGMax_mA", "SpecCurrentMin_mA")
            }
            //4.SpecCurrentMin_mA<LEDCurrentV<SpecCurrentVMax_mA
            case "Scan3LEDInfo_SiriusFW^DLEDCurrentV" => {
                getMaxMinSpec(compare, "SpecCurrentVMax_mA", "SpecCurrentMin_mA")
            }
            //5.SpecLevelGMin_Bits8<LevelAchievedG<SpecLevelGMax_Bits8
            case "Scan3LEDInfo_SiriusFW^DLevelAchievedG" => {
                getMaxMinSpec(compare, "SpecLevelGMax_Bits8", "SpecLevelGMin_Bits8")
            }
            //6.SpecLevelOMin_Bits8<LevelAchievedO<SpecLevelOMax_Bits8
            case "Scan3LEDInfo_SiriusFW^DLevelAchievedO" => {
                getMaxMinSpec(compare, "SpecLevelOMax_Bits8", "SpecLevelOMin_Bits8")
            }
            //7.SpecLevelVMin_Bits8<LevelAchievedV<SpecLevelVMax_Bits8
            case "Scan3LEDInfo_SiriusFW^DLevelAchievedV" => {
                getMaxMinSpec(compare, "SpecLevelVMax_Bits8", "SpecLevelVMin_Bits8")
            }
            //Scan3PRNU_SiriusFW
            //1.WhiteMaxR<SpecWhiteMax_Bits12
            case "Scan3PRNU_SiriusFW^DWhiteMaxR" => {
                getMaxSpec(compare, "SpecWhiteMax_Bits12")
            }
            //2.WhiteMaxG<SpecWhiteMax_Bits12
            case "Scan3PRNU_SiriusFW^DWhiteMaxG" => {
                getMaxSpec(compare, "SpecWhiteMax_Bits12")
            }
            //3.WhiteMaxB<SpecWhiteMax_Bits12
            case "Scan3PRNU_SiriusFW^DWhiteMaxB" => {
                getMaxSpec(compare, "SpecWhiteMax_Bits12")
            }
            //4.WhiteMinR>SpecWhiteMin_Bits12
            case "Scan3PRNU_SiriusFW^DWhiteMinR" => {
                getMinSpec(compare, "SpecWhiteMin_Bits12")
            }
            //5.WhiteMinG>SpecWhiteMin_Bits12
            case "Scan3PRNU_SiriusFW^DWhiteMinG" => {
                getMinSpec(compare, "SpecWhiteMin_Bits12")
            }
            //6.WhiteMinB>SpecWhiteMin_Bits12
            case "Scan3PRNU_SiriusFW^DWhiteMinB" => {
                getMinSpec(compare, "SpecWhiteMin_Bits12")
            }
            //7.WhiteMaxR-WhiteMinR<SpecWhiteMinOfMax_Bits12, 非單一測項的spec
            //8.WhiteMaxG-WhiteMinG<SpecWhiteMinOfMax_Bits12, 非單一測項的spec
            //9.WhiteMaxB-WhiteMinB<SpecWhiteMinOfMax_Bits12, 非單一測項的spec

            //10.DarkMaxRGB<SpecDarkMax_Bits12
            case "Scan3PRNU_SiriusFW^DDarkMaxRGB" => {
                getMaxSpec(compare, "SpecDarkMax_Bits12")
            }
            //11.DarkMinRGB>SpecDarkMin_Bits12
            case "Scan3PRNU_SiriusFW^DDarkMinRGB" => {
                getMinSpec(compare, "SpecDarkMin_Bits12")
            }
            //12.DarkDelta<SpecDarkDelta_Bits12
            case "Scan3PRNU_SiriusFW^DDarkDelta" => {
                getMaxSpec(compare, "SpecDarkDelta_Bits12")
            }
            //13.SpecMinDocOffsetX_Pix300<DocOffsetX<SpecMaxDocOffsetX_Pix300
            case "Scan3PRNU_SiriusFW^DDocOffsetX" => {
                getMaxMinSpec(compare, "SpecMaxDocOffsetX_Pix300", "SpecMinDocOffsetX_Pix300")
            }
            //14.SpecMinDocOffsetY_Pix300<DocOffsetY<SpecMaxDocOffsetY_Pix300
            case "Scan3PRNU_SiriusFW^DDocOffsetY" => {
                getMaxMinSpec(compare, "SpecMaxDocOffsetY_Quads", "SpecMinDocOffsetY_Quads")
            }
            //Scan3MOMS
            //1.SpecTopMarginErrorMin_mm<TopMarginError_mm<SpecTopMarginErrorMax_mm
            case "Scan3PRNU_SiriusFW^DTopMarginError_mm" => {
                getMaxMinSpec(compare, "SpecTopMarginErrorMax_mm", "SpecTopMarginErrorMin_mm")
            }
            //2.SpecBotMarginErrorMin_mm<BotMarginError_mm<SpecBotMarginErrorMax_mm
            case "Scan3MOMS^DBotMarginError_mm" => {
                getMaxMinSpec(compare, "SpecBotMarginErrorMax_mm", "SpecBotMarginErrorMin_mm")
            }
            //3.SpecLftMarginErrorMin_mm<LftMarginErrorTop_mm<SpecLftMarginErrorMax_mm
            case "Scan3MOMS^DLftMarginErrorTop_mm" => {
                getMaxMinSpec(compare, "SpecLftMarginErrorMax_mm", "SpecLftMarginErrorMin_mm")
            }
            //4.SpecRgtMarginErrorMin_mm<RgtMarginErrorTop_mm<SpecRgtMarginErrorMax_mm
            case "Scan3MOMS^DRgtMarginErrorTop_mm" => {
                getMaxMinSpec(compare, "SpecRgtMarginErrorMax_mm", "SpecRgtMarginErrorMin_mm")
            }
            //5.SpecLftMarginErrorMin_mm<LftMarginErrorBot_mm<SpecLftMarginErrorMax_mm
            case "Scan3MOMS^DLftMarginErrorBot_mm" => {
                getMaxMinSpec(compare, "SpecLftMarginErrorMax_mm", "SpecLftMarginErrorMin_mm")
            }
            //6.SpecRgtMarginErrorMin_mm<RgtMarginErrorBot_mm<SpecRgtMarginErrorMax_mm
            case "Scan3MOMS^DRgtMarginErrorBot_mm" => {
                getMaxMinSpec(compare, "SpecRgtMarginErrorMax_mm", "SpecRgtMarginErrorMin_mm")
            }
            //7.CtrXMarginError1_mm<SpecCenterXError_mm
            case "Scan3MOMS^DCtrXMarginError1_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //8.CtrXMarginError2_mm<SpecCenterXError_mm
            case "Scan3MOMS^DCtrXMarginError2_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //9.CtrXMarginError3_mm<SpecCenterXError_mm
            case "Scan3MOMS^DCtrXMarginError3_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //10.CtrXMarginError4_mm<SpecCenterXError_mm
            case "Scan3MOMS^DCtrXMarginError4_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //11.CtrXMarginError5_mm<SpecCenterXError_mm
            case "Scan3MOMS^DCtrXMarginError5_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //12.CtrXMarginError6_mm<SpecCenterXError_mm
            case "Scan3MOMS^DCtrXMarginError6_mm" => {
                getMaxSpec(compare, "SpecCenterXError_mm")
            }
            //13.TopSkewError_Deg<SpecTopSkewError_Deg
            case "Scan3MOMS^DTopSkewError_Deg" => {
                getMaxSpec(compare, "SpecTopSkewError_Deg")
            }
            //14.FeedSkewError_Deg<SpecFeedSkewError_Deg
            case "Scan3MOMS^DFeedSkewError_Deg" => {
                getMaxSpec(compare, "SpecFeedSkewError_Deg")
            }
            //15.SpecMagXErrorMin_Per<MagXError_Percent<SpecMagXErrorMax_Per
            case "Scan3MOMS^DMagXError_Percent" => {
                getMaxMinSpec(compare, "SpecMagXErrorMax_Per", "SpecMagXErrorMin_Per")
            }
            //16.SpecMagYErrorMin_Per<MagYError_Percent<SpecMagYErrorMax_Per
            case "Scan3MOMS^DMagYError_Percent" => {
                getMaxMinSpec(compare, "SpecMagYErrorMax_Per", "SpecMagYErrorMin_Per")
            }
            //17.OrthogError1_Deg<SpecOrthog_Deg
            case "Scan3MOMS^DOrthogError1_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //18.OrthogError2_Deg<SpecOrthog_Deg
            case "Scan3MOMS^DOrthogError2_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //19.OrthogError3_Deg<SpecOrthog_Deg
            case "Scan3MOMS^DOrthogError3_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //20.OrthogError4_Deg<SpecOrthog_Deg
            case "Scan3MOMS^DOrthogError4_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //21.OrthogError5_Deg<SpecOrthog_Deg
            case "Scan3MOMS^DOrthogError5_Deg" => {
                getMaxSpec(compare, "SpecOrthog_Deg")
            }
            //22.SpecMinDocOffsetX_Pix300<DocOffsetX<SpecMaxDocOffsetX_Pix300
            case "Scan3MOMS^DDocOffsetX" => {
                getMaxMinSpec(compare, "SpecMaxDocOffsetX_Pix300", "SpecMinDocOffsetX_Pix300")
            }
            //23.SpecMinDocOffsetY_Quads<DocOffsetY<SpecMaxDocOffsetY_Quads
            case "Scan3MOMS^DDocOffsetY" => {
                getMaxMinSpec(compare, "SpecMaxDocOffsetY_Quads", "SpecMinDocOffsetY_Quads")
            }
            //Scan3StreakRGB
            //1.LightStreakRed_MaxDelta_Zone1<SpecLightStreakinRed_Bits8
            case "Scan3StreakRGB^DLightStreakRed_MaxDelta_Zone1" => {
                getMaxSpec(compare, "SpecLightStreakinRed_Bits8")
            }
            //2.LightStreakGreen_MaxDelta_Zone1<SpecLightStreakinGreen_Bits8
            case "Scan3StreakRGB^DLightStreakGreen_MaxDelta_Zone1" => {
                getMaxSpec(compare, "SpecLightStreakinGreen_Bits8")
            }
            //3.LightStreakBlue_MaxDelta_Zone1<SpecLightStreakinBlue_Bits8
            case "Scan3StreakRGB^DLightStreakBlue_MaxDelta_Zone1" => {
                getMaxSpec(compare, "SpecLightStreakinBlue_Bits8")
            }
            //Scan3IL
            //1.MaxDelta_RedBar_Ch1<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_RedBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //2.MaxDelta_RedBar_Ch2<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_RedBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //3.MaxDelta_RedBar_Ch3<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_RedBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //4.MaxDelta_GreBar_Ch1<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_GreBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //5.MaxDelta_GreBar_Ch2<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_GreBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //6.MaxDelta_GreBar_Ch3<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_GreBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //7.MaxDelta_BluBar_Ch1<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_BluBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //8.MaxDelta_BluBar_Ch2<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_BluBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //9.MaxDelta_BluBar_Ch3<SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxDelta_BluBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipTolFromNom_Bits8")
            }
            //10.MaxAvg_RedBar_Ch1<SpecRedNomCh123_Bits8.1+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_RedBar_Ch1" => {
                getMaxComputationSpec(compare, "SpecRedNomCh123_Bits8", "SpecMaxChipTolFromNom_Bits8")
            }
            //11.MaxAvg_RedBar_Ch2<SpecRedNomCh123_Bits8.2+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_RedBar_Ch2" => {
                getMaxComputationSpec(compare, "SpecRedNomCh123_Bits8.2", "SpecMaxChipTolFromNom_Bits8")
            }
            //12.MaxAvg_RedBar_Ch3<SpecRedNomCh123_Bits8.3+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_RedBar_Ch3" => {
                getMaxComputationSpec(compare, "SpecRedNomCh123_Bits8.3", "SpecMaxChipTolFromNom_Bits8")
            }
            //13.MinAvg_RedBar_Ch1>SpecRedNomCh123_Bits8.1-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_RedBar_Ch1" => {
                getMinComputationSpec(compare, "SpecRedNomCh123_Bits8.1", "SpecMaxChipTolFromNom_Bits8")
            }
            //14.MinAvg_RedBar_Ch2>SpecRedNomCh123_Bits8.2-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_RedBar_Ch2" => {
                getMinComputationSpec(compare, "SpecRedNomCh123_Bits8.2", "SpecMaxChipTolFromNom_Bits8")
            }
            //15.MinAvg_RedBar_Ch3>SpecRedNomCh123_Bits8.3-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_RedBar_Ch3" => {
                getMinComputationSpec(compare, "SpecRedNomCh123_Bits8.3", "SpecMaxChipTolFromNom_Bits8")
            }
            //16.MaxAvg_GreBar_Ch1<SpecGreenNomCh123_Bits8.1+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_GreBar_Ch1" => {
                getMaxComputationSpec(compare, "SpecGreenNomCh123_Bits8.1", "SpecMaxChipTolFromNom_Bits8")
            }
            //17.MaxAvg_GreBar_Ch2<SpecGreenNomCh123_Bits8.2+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_GreBar_Ch2" => {
                getMaxComputationSpec(compare, "SpecGreenNomCh123_Bits8.2", "SpecMaxChipTolFromNom_Bits8")
            }
            //18.MaxAvg_GreBar_Ch3<SpecGreenNomCh123_Bits8.3+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_GreBar_Ch3" => {
                getMaxComputationSpec(compare, "SpecGreenNomCh123_Bits8.3", "SpecMaxChipTolFromNom_Bits8")
            }
            //19.MinAvg_GreBar_Ch1>SpecGreenNomCh123_Bits8.1-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_GreBar_Ch1" => {
                getMinComputationSpec(compare, "SpecGreenNomCh123_Bits8.1", "SpecMaxChipTolFromNom_Bits8")
            }
            //20.MinAvg_GreBar_Ch2>SpecGreenNomCh123_Bits8.2-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_GreBar_Ch2" => {
                getMinComputationSpec(compare, "SpecGreenNomCh123_Bits8.2", "SpecMaxChipTolFromNom_Bits8")
            }
            //21.MinAvg_GreBar_Ch3>SpecGreenNomCh123_Bits8.3-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_GreBar_Ch3" => {
                getMinComputationSpec(compare, "SpecGreenNomCh123_Bits8.3", "SpecMaxChipTolFromNom_Bits8")
                /*var lowerValue = compare.filter(testparm=> testparm.contains("SpecGreenNomCh123_Bits8.3")).mkString.split("=")(1).toFloat
                var lowerSub = compare.filter(testparm=> testparm.contains("SpecMaxChipTolFromNom_Bits8")).mkString.split("=")(1).toFloat

                var upper = ""
                var lower = (lowerValue - lowerSub).toString
                Vector(upper, lower)*/
            }
            //22.MaxAvg_BluBar_Ch1<SpecBlueNomCh123_Bits8.1+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_BluBar_Ch1" => {
                getMaxComputationSpec(compare, "SpecBlueNomCh123_Bits8.1", "SpecMaxChipTolFromNom_Bits8")
            }
            //23.MaxAvg_BluBar_Ch2<SpecBlueNomCh123_Bits8.2+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_BluBar_Ch2" => {
                getMaxComputationSpec(compare, "SpecBlueNomCh123_Bits8.2", "SpecMaxChipTolFromNom_Bits8")
            }
            //24.MaxAvg_BluBar_Ch3<SpecBlueNomCh123_Bits8.3+SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMaxAvg_BluBar_Ch3" => {
                getMaxComputationSpec(compare, "SpecBlueNomCh123_Bits8.3", "SpecMaxChipTolFromNom_Bits8")
                /*
                var upper = compare.filter(testparm=> testparm.contains("SpecBlueNomCh123_Bits8.3") | testparm.contains("SpecMaxChipTolFromNom_Bits8"))
                  .map(_.split("=")(1).toFloat).sum.toString
                var lower = ""
                Vector(upper, lower)*/
            }
            //25.MinAvg_BluBar_Ch1>SpecBlueNomCh123_Bits8.1-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_BluBar_Ch1" => {
                getMinComputationSpec(compare, "SpecBlueNomCh123_Bits8.1", "SpecMaxChipTolFromNom_Bits8")
            }
            //26.MinAvg_BluBar_Ch2>SpecBlueNomCh123_Bits8.2-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_BluBar_Ch2" => {
                getMinComputationSpec(compare, "SpecBlueNomCh123_Bits8.2", "SpecMaxChipTolFromNom_Bits8")
            }
            //27.MinAvg_BluBar_Ch3>SpecBlueNomCh123_Bits8.3-SpecMaxChipTolFromNom_Bits8
            case "Scan3IL^DMinAvg_BluBar_Ch3" => {
                getMinComputationSpec(compare, "SpecBlueNomCh123_Bits8.3", "SpecMaxChipTolFromNom_Bits8")
            }
            //28.MaxStd_RedBar_Ch1<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_RedBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //29.MaxStd_RedBar_Ch2<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_RedBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //30.MaxStd_RedBar_Ch3<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_RedBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //31.MaxStd_GreBar_Ch1<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_GreBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //32.MaxStd_GreBar_Ch2<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_GreBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //33.MaxStd_GreBar_Ch3<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_GreBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //34.MaxStd_BluBar_Ch1<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_BluBar_Ch1" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //35.MaxStd_BluBar_Ch2<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_BluBar_Ch2" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //36.MaxStd_BluBar_Ch3<SpecMaxChipStd_Bits8
            case "Scan3IL^DMaxStd_BluBar_Ch3" => {
                getMaxSpec(compare, "SpecMaxChipStd_Bits8")
            }
            //Scan3StreakFlare
            //1.LightStreakinBlack_MaxDelta_1<SpecLightStreakinBlack_Bits8
            case "Scan3StreakFlare^DLightStreakinBlack_MaxDelta_1" => {
                getMaxSpec(compare, "SpecLightStreakinBlack_Bits8")
            }
            //2.LightStreakinBlack_MaxDelta_2<SpecLightStreakinBlack_Bits8
            case "Scan3StreakFlare^DLightStreakinBlack_MaxDelta_2" => {
                getMaxSpec(compare, "SpecLightStreakinBlack_Bits8")
            }
            //3.LightStreakinWhite_MaxDelta<SpecLightStreakinWhite_Bits8
            case "Scan3StreakFlare^DLightStreakinWhite_MaxDelta" => {
                getMaxSpec(compare, "SpecLightStreakinWhite_Bits8")
            }
            //4.DarkStreakinBlack_MaxDelta<SpecDarkStreakinBlack_Bits8
            case "Scan3StreakFlare^DDarkStreakinBlack_MaxDelta" => {
                getMaxSpec(compare, "SpecDarkStreakinBlack_Bits8")
            }
            //5.DarkStreakinWhite_MaxDelta<SpecDarkStreakinWhite_Bits8
            case "Scan3StreakFlare^DDarkStreakinWhite_MaxDelta" => {
                getMaxSpec(compare, "SpecDarkStreakinWhite_Bits8")
            }
            //6.FlareStreak_MaxDelta<SpecFlareStreak_Bits8
            case "Scan3StreakFlare^DFlareStreak_MaxDelta" => {
                getMaxSpec(compare, "SpecFlareStreak_Bits8")
            }
            //Scan3SLI
            //1.MaxFullCurvature_Pix<SpecMaxFullCurvature_Pix600
            case "Scan3SLI^DMaxFullCurvature_Pix" => {
                getMaxSpec(compare, "SpecMaxFullCurvature_Pix600")
            }
            //2.MaxLocalCurvature_Pix<SpecMaxLocalCurvature_Pix600
            case "Scan3SLI^DMaxLocalCurvature_Pix" => {
                getMaxSpec(compare, "SpecMaxLocalCurvature_Pix600")
            }
            //3.MaxStep_Microns<SpecMaxStep_Microns
            case "Scan3SLI^DMaxStep_Microns" => {
                getMaxSpec(compare, "SpecMaxStep_Microns")
            }
            //4.MaxSlantChange_MicronsPerMM<SpecMaxSlantDelta_MicronsPerMM
            case "Scan3SLI^DMaxSlantChange_MicronsPerMM" => {
                getMaxSpec(compare, "SpecMaxSlantDelta_MicronsPerMM")
            }
            //Scan3MTF　
            //1.MinMTF>SpecMinMTF
            case "Scan3MTF^DMinMTF" => {
                getMinSpec(compare, "SpecMinMTF")
            }
            //2.MaxMTF<SpecMaxMTF
            case "Scan3MTF^DMaxMTF" => {
                getMaxSpec(compare, "SpecMaxMTF")
            }
            //Scan3BPRWPR.1
            //1.MaxAvgL<SpecMaxAvgL_Bits8
            case "Scan3BPRWPR.1^DMaxAvgL" => {
                getMaxSpec(compare, "SpecMaxAvgL_Bits8")
            }
            //2.MinAvgL>SpecMinAvgL_Bits8
            case "Scan3BPRWPR.1^DMinAvgL" => {
                getMinSpec(compare, "SpecMinAvgL_Bits8")
            }
            //3.MaxAvgC<SpecMaxAvgC_Bits8
            case "Scan3BPRWPR.1^DMaxAvgC" => {
                getMaxSpec(compare, "SpecMaxAvgC_Bits8")
            }
            //4.MaxDeltaL<SpecMaxDeltaL_Bits8
            case "Scan3BPRWPR.1^DMaxDeltaL" => {
                getMaxSpec(compare, "SpecMaxDeltaL_Bits8")
            }
            //5.MaxDeltaAB<SpecMaxDeltaAB_Bits8
            case "Scan3BPRWPR.1^DMaxDeltaAB" => {
                getMaxSpec(compare, "SpecMaxDeltaAB_Bits8")
            }
            //6.MaxNoiseL<SpecMaxNoiseL_Bits8
            case "Scan3BPRWPR.1^DMaxNoiseL" => {
                getMaxSpec(compare, "SpecMaxNoiseL_Bits8")
            }
            //7.MaxNoiseA<SpecMaxNoiseAB_Bits8
            case "Scan3BPRWPR.1^DMaxNoiseA" => {
                getMaxSpec(compare, "SpecMaxNoiseAB_Bits8")
            }
            //8.MaxNoiseB<SpecMaxNoiseAB_Bits8
            case "Scan3BPRWPR.1^DMaxNoiseB" => {
                getMaxSpec(compare, "SpecMaxNoiseAB_Bits8")
            }
            //9.MaxRowNoiseL<SpecMaxRowNoiseL_Bits8
            case "Scan3BPRWPR.1^DMaxRowNoiseL" => {
                getMaxSpec(compare, "SpecMaxRowNoiseL_Bits8")
            }
            //10.MaxRowNoiseA<SpecMaxRowNoiseAB_Bits8
            case "Scan3BPRWPR.1^DMaxRowNoiseA" => {
                getMaxSpec(compare, "SpecMaxRowNoiseAB_Bits8")
            }
            //11.MaxRowNoiseB<SpecMaxRowNoiseAB_Bits8
            case "Scan3BPRWPR.1^DMaxRowNoiseB" => {
                getMaxSpec(compare, "SpecMaxRowNoiseAB_Bits8")
            }
            //12.ColNoiseStd<SpecMaxColStd_Bits8
            case "Scan3BPRWPR.1^DColNoiseStd" => {
                getMaxSpec(compare, "SpecMaxColStd_Bits8")
            }
            //Scan3BPRWPR.2
            //1.MaxAvgL<SpecMaxAvgL_Bits8
            case "Scan3BPRWPR.2^DMaxAvgL" => {
                getMaxSpec(compare, "SpecMaxAvgL_Bits8")
            }
            //2.MinAvgL>SpecMinAvgL_Bits8
            case "Scan3BPRWPR.2^DMinAvgL" => {
                getMinSpec(compare, "SpecMinAvgL_Bits8")
            }
            //3.MaxAvgC<SpecMaxAvgC_Bits8
            case "Scan3BPRWPR.2^DMaxAvgC" => {
                getMaxSpec(compare, "SpecMaxAvgC_Bits8")
            }
            //4.MaxDeltaL<SpecMaxDeltaL_Bits8
            case "Scan3BPRWPR.2^DMaxDeltaL" => {
                getMaxSpec(compare, "SpecMaxDeltaL_Bits8")
            }
            //5.MaxDeltaAB<SpecMaxDeltaAB_Bits8
            case "Scan3BPRWPR.2^DMaxDeltaAB" => {
                getMaxSpec(compare, "SpecMaxDeltaAB_Bits8")
            }
            //6.MaxNoiseL<SpecMaxNoiseL_Bits8
            case "Scan3BPRWPR.2^DMaxNoiseL" => {
                getMaxSpec(compare, "SpecMaxNoiseL_Bits8")
            }
            //7.MaxNoiseA<SpecMaxNoiseAB_Bits8
            case "Scan3BPRWPR.2^DMaxNoiseA" => {
                getMaxSpec(compare, "SpecMaxNoiseAB_Bits8")
            }
            //8.MaxNoiseB<SpecMaxNoiseAB_Bits8
            case "Scan3BPRWPR.2^DMaxNoiseB" => {
                getMaxSpec(compare, "SpecMaxNoiseAB_Bits8")
            }

            case "PcaVerifyCustomerDefaults_Taiji^DActualFWUpdateDSID" | "MechPowerOffUUT_WuDang^DActualFWUpdateDSID" => {
                getEqualSpec(compare, "FWUpdateDSID")
            }

            case _ => Vector("", "")
        }

    }

    def getSpecBoundary(compare: Seq[String], spec: String): String = {
        compare.filter(testparm => testparm.contains(spec)).mkString.split("=")(1)
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
    def getMaxComputationSpec(compare: Seq[String], specMax1: String, specMax2: String): Vector[String] = {
        var upper = compare.filter(testparm=> testparm.contains(specMax1) | testparm.contains(specMax2))
          .map(_.split("=")(1).toFloat).sum.toString
        var lower = ""
        Vector(upper, lower)
    }
    //下界特別計算
    def getMinComputationSpec(compare: Seq[String], specMinValue: String, specMinSub: String): Vector[String] = {
        var lowerValue = compare.filter(testparm=> testparm.contains(specMinValue)).mkString.split("=")(1).toFloat
        var lowerSub = compare.filter(testparm=> testparm.contains(specMinSub)).mkString.split("=")(1).toFloat

        var upper = ""
        var lower = (lowerValue - lowerSub).toString
        Vector(upper, lower)
    }

}
