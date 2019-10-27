package com.alain898.book.realtimestreaming.chapter5;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.stat.regression.MillerUpdatingRegression;
import org.apache.commons.math3.stat.regression.UpdatingMultipleLinearRegression;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class NeuralNetworkRegressionTest {


    public void test() {
//        double[] prices = new double[1000];
//        for (int i = 0; i < prices.length; i++) {
//            prices[i] = Math.sin(2 * Math.PI / 45 * i) + new Random().nextGaussian() * 0.5;
//        }

        double[] prices = new double[]{1530.84, 1538.32, 1320.63, 1319.87, 1334.68, 1332.06, 1372.07, 1397.38, 1384.86, 1386.31, 1466.85, 1459.92, 1485.62, 1478.72, 1482.68, 1454.89, 1460.29, 1450.62, 1478.51, 1496.16, 1501.2, 1499.81, 1480.11, 1492.76, 1510.67, 1504.34, 1510.95, 1496.83, 1496.47, 1511.08, 1509.49, 1478.87, 1481.97, 1511.59, 1513.18, 1513.7, 1512.14, 1525.48, 1524.32, 1517.71, 1498.54, 1493.17, 1469.31, 1469.9, 1475.24, 1465.08, 1466.11, 1469.27, 1459.89, 1470.13, 1465.1, 1474.66, 1477.77, 1462.61, 1456.27, 1499.15, 1491.94, 1510.58, 1522.76, 1521.19, 1511.73, 1521.13, 1514.92, 1520.55, 1543.75, 1551.26, 1577.49, 1616.15, 1631.65, 1606.77, 1613.6, 1603.08, 1572.17, 1568.69, 1542.32, 1504.05, 1487.34, 1538.15, 1509.52, 1521.44, 1532.48, 1485.31, 1522.91, 1555.52, 1554.85, 1555.57, 1536.29, 1533.48, 1546.11, 1560.85, 1569.51, 1568.34, 1551.81, 1568.48, 1576.26, 1576.53, 1563.2, 1574.11, 1556.83, 1539.11, 1530.55, 1538.76, 1565.84, 1562.11, 1566.77, 1559.7, 1557.09, 1553.88, 1538.75, 1529.47, 1511.36, 1513.46, 1502.08, 1515.38, 1497.05, 1486.44, 1484.7, 1499.68, 1504.44, 1502.52, 1501.85, 1512.22, 1503.31, 1531.93, 1529.06, 1521.78, 1525.44, 1529.49, 1539.36, 1507.4, 1496.17, 1498.48, 1502.15, 1493.64, 1477.52, 1470.39, 1477.52, 1479.65, 1476.91, 1475.15, 1494.36, 1494.48, 1486.25, 1487.58, 1471.27, 1474.41, 1479.28, 1466.42, 1456.28, 1450.2, 1445.09, 1449.71, 1442.32, 1441.26, 1439.09, 1422.18, 1425.21, 1425.15, 1415.69, 1421.98, 1449.82, 1446.51, 1448.0, 1447.43, 1431.32, 1425.73, 1420.68, 1423.12, 1423.87, 1409.16, 1398.1, 1406.1, 1390.34, 1390.48, 1391.92, 1381.78, 1390.3, 1394.24, 1376.6, 1371.23, 1355.71, 1367.16, 1372.06, 1369.32, 1404.22, 1400.0, 1388.43, 1383.1, 1365.93, 1370.8, 1359.72, 1364.6, 1398.07, 1385.87, 1382.19, 1369.34, 1369.03, 1361.49, 1344.45, 1348.3, 1364.35, 1389.9, 1385.11, 1356.65, 1335.75, 1340.83, 1343.61, 1318.66, 1319.69, 1331.21, 1328.03, 1316.85, 1339.11, 1377.1, 1361.94, 1403.97, 1408.81, 1412.35, 1389.52, 1397.66, 1432.77, 1435.47, 1445.72, 1452.81, 1451.18, 1436.84, 1443.54, 1469.68, 1469.9, 1470.47, 1470.36, 1466.34, 1454.92, 1457.3, 1446.29, 1492.96, 1500.64, 1513.82, 1512.6, 1514.78, 1496.43, 1512.71, 1497.04};

        int numberOfVariables = 10;
        UpdatingMultipleLinearRegression rm = new MillerUpdatingRegression(numberOfVariables, true);
        Queue<Double> xPrices = new LinkedList<>();
        double[] predictPrices = new double[prices.length];
        for (int i = 0; i < prices.length; i++) {
            double price = prices[i];

            // numberOfVariables not enough, so pass and continue
            if (i < numberOfVariables) {
                xPrices.add(price);
                predictPrices[i] = 0;
                continue;
            }

            if (i <= numberOfVariables * 2 + 1) {
                // observations not enough, so pass and continue
                predictPrices[i] = 0;
            } else {
                // predict according model
                double params[] = rm.regress().getParameterEstimates();
                List<Double> xpList = new LinkedList<>();
                xpList.add(1d);
                xpList.addAll(xPrices);
                double[] x_p = ArrayUtils.toPrimitive(xpList.toArray(new Double[0]));
                double y_p = new ArrayRealVector(x_p).dotProduct(new ArrayRealVector(params));
                predictPrices[i] = y_p;
            }

            // update model
            double[] x = ArrayUtils.toPrimitive(xPrices.toArray(new Double[0]));
            double y = price;
            rm.addObservation(x, y);
            xPrices.add(price);
            xPrices.remove();
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("prices", prices);
        jsonObject.put("predictPrices", predictPrices);
        System.out.println(jsonObject);
    }

    public static void main(String[] args) {
        NeuralNetworkRegressionTest test = new NeuralNetworkRegressionTest();
        test.test();
    }
}
