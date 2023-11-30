import React from 'react';
import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const BarChartGraph = ({ data, type }) => {
    return (
        <ResponsiveContainer width="95%" height={400}>

            <BarChart height={500} data={data}>
                <XAxis dataKey="year" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey={type} fill="#9DCAFF" barSize={20} />
            </BarChart>
        </ResponsiveContainer>
    );
}

export default BarChartGraph;
