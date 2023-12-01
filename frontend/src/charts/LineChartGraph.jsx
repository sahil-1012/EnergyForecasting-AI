import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import useMediaQuery from '@mui/material/useMediaQuery';

const LineGraph = ({ data, dataKey }) => {
    const isLaptop = useMediaQuery('(min-width: 1000px)');
    const isSmallScreen = useMediaQuery('(max-width: 450px)');

    return (
        <ResponsiveContainer width='100%' aspect={11 / 3}>
            <LineChart width="100%" height={300} data={data}>
                {isLaptop ? (
                    <XAxis dataKey={dataKey} interval={1} />
                ) : (
                    <XAxis dataKey={dataKey} />
                )}
                <YAxis />
                {!isSmallScreen && <Tooltip />}
                <Line type="monotone" dataKey="emission" stroke="#8884d8" />
            </LineChart>
        </ResponsiveContainer>
    );
};

export default LineGraph;
