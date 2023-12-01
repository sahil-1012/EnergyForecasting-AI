import React from 'react';
import { PieChart, Pie, Cell, Legend, Tooltip } from 'recharts';

const COLORS = [
    '#480740', '#641662', '#7B2C81', '#8F499D', '#A546DB9', '#BF98D5', '#DFCAF1',
    '#12A4D9', '#6FD3FF', '#9DCAFF',
].map(color => adjustBrightness(color, -20)); // Adjust brightness for contrast

function adjustBrightness(hex, percent) {
    const num = parseInt(hex.slice(1), 16);
    const r = (num >> 16) + percent;
    const g = ((num >> 8) & 0x00FF) + percent;
    const b = (num & 0x0000FF) + percent;

    const adjustedColor = (
        (1 << 24) + (Math.min(255, Math.max(0, r)) << 16) +
        (Math.min(255, Math.max(0, g)) << 8) + Math.min(255, Math.max(0, b))
    ).toString(16).slice(1);

    return `#${adjustedColor}`;
}

export const PieChartGraph = ({ data }) => {
    // Sort the data by "value" in descending order
    const sortedData = data?.slice()?.sort((a, b) => b?.value - a?.value);

    return (
        <PieChart width={500} height={550} >
            <Pie
                data={sortedData}
                cx={200}
                cy={200}
                innerRadius={0}
                outerRadius={200}
                dataKey="value"
            >
                {sortedData?.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
            </Pie>

            {/* <div className='w-full flex'> */}
                <Legend
                style={{position:'relative'}}
                    payload={sortedData?.map((entry, index) => ({
                        value: entry.country,
                        type: 'circle',
                        id: `color-${index}`,
                        color: COLORS[index % COLORS.length],
                    }))}
                />
            {/* </div> */}

            <Tooltip
                formatter={(value, name, props) => [value, props.payload.country]}
            />
        </PieChart>
    );
};