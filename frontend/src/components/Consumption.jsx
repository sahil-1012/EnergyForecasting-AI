import React, { useEffect, useState } from 'react'
import { PieChartGraph } from '../charts/PieChartGraph';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';


const Consumption = ({ value }) => {
    const [data, setData] = useState();
    const [loading, setLoading] = useState(true);
    const [year, setYear] = useState(2022);
    const [consumptionType, setConsumptionType] = useState('total');

    useEffect(() => {
        const fetchData = async () => {
            try {
                const response = await fetch(`http://localhost:4000/api/getTotalConsumption?year=${year}`);

                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                const result = await response.json();
                console.log(result);
                setData(result);
            } catch (error) {
                console.error(error.message);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [year, consumptionType]);

    const startYear = 1990;
    const endYear = new Date().getFullYear() - 1;

    const years = Array.from({ length: endYear - startYear + 1 }, (_, index) => startYear + index);

    return (
        <div className="bg-blue-50 h-full grid grid-cols-2">
            <div className='p-5 md:p-20 col-span-full lg:col-span-1'>
                <h2 className='text-blue-600 font-graphik  font-bold text-3xl md:text-4xl'>Energy Statistics Data Browser</h2>
                <p class="text-slate-500 my-2 w-full">
                    Access the most extensive selection of IEA statistics with charts and tables on 16 energy topics for over 170 countries and regions.
                </p>
            </div>

            <div className='m-10 col-span-full lg:col-span-1 bg-white p-10 rounded-xl'>
                <div className='w-full mb-5 grid grid-cols-2 gap-20'>
                    <div className='col-span-1 '>
                        <FormControl fullWidth >
                            <InputLabel id="demo-simple-select-label">Year</InputLabel>
                            <Select
                                value={year}
                                label="Year"
                                onChange={(e) => setYear(e.target.value)}
                                MenuProps={{
                                    PaperProps: {
                                        style: {
                                            maxHeight: 200,
                                        },
                                    },
                                }}
                            >
                                {years.map((year) => (
                                    <MenuItem key={year} value={year}>
                                        {year}
                                    </MenuItem>
                                ))}
                            </Select>

                        </FormControl>
                    </div>

                    <div className='col-span-1'>
                        <FormControl fullWidth >
                            <InputLabel id="demo-simple-select-label">Type</InputLabel>
                            <Select
                                value={consumptionType}
                                label="Type"
                                onChange={(e) => setConsumptionType(e.target.value)}
                                MenuProps={{
                                    PaperProps: {
                                        style: {
                                            maxHeight: 200,
                                        },
                                    },
                                }}
                            >
                                <MenuItem value='total'>Total</MenuItem>
                                <MenuItem value='coal'>Coal</MenuItem>
                            </Select>

                        </FormControl>
                    </div>
                </div>

                <div className='flex w-full items-center justify-center'>
                    <PieChartGraph data={data} />
                </div>
            </div>
        </div>
    )
}

export default Consumption;