package dev.lakehouse.udf;

import com.uber.h3core.H3Core;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * h3_cell(lat, lon, resolution) -> H3 cell address (hex string), or NULL for invalid input.
 *
 * Resolution 8 is roughly 0.7 km^2 per hexagon (edge ~460 m): a sensible "pickup zone"
 * granularity for surge / supply-demand features.
 *
 * H3Core is not serializable and loads a native library, so it is created in open(),
 * once per task, never in the constructor.
 */
public class H3Cell extends ScalarFunction {

    private transient H3Core h3;

    @Override
    public void open(FunctionContext context) throws Exception {
        h3 = H3Core.newInstance();
    }

    public String eval(Double lat, Double lon, Integer resolution) {
        if (lat == null || lon == null || resolution == null) {
            return null;
        }
        if (lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0) {
            return null;
        }
        if (resolution < 0 || resolution > 15) {
            return null;
        }
        return h3.latLngToCellAddress(lat, lon, resolution);
    }
}
