import React, { useEffect, useState } from 'react'
import { PieChartGraph } from '../charts/PieChartGraph';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';
import BarChartGraph from '../charts/BarChartGraph';


const Consumption = ({ value }) => {
    const [data, setData] = useState();
    const [barData, setBarData] = useState();
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
                setData(result);
            } catch (error) {
                console.error(error.message);
            } finally {
                setLoading(false);
            }
        };

        fetchData();
    }, [year, consumptionType]);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const response = await fetch('http://localhost:4000/api/getTotalConsumptionOverYears');
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }

                const result = await response.json();
                setBarData(result)
            } catch (error) {
                console.error(error.message);
            }
        };

        fetchData();
    }, []);


    const startYear = 1990;
    const endYear = new Date().getFullYear() - 1;

    const years = Array.from({ length: endYear - startYear + 1 }, (_, index) => startYear + index);

    return (
        <div className="bg-blue-50 h-full grid grid-cols-2">
            <div className='p-5 md:p-20 col-span-full lg:col-span-1'>
                <h2 className='text-blue-600 font-graphik  font-bold text-3xl md:text-4xl'>Energy Consumption Statistics</h2>
                <p class="text-slate-500 my-4 w-full">
                    Explore comprehensive data on global energy consumption over the years.
                    Gain insights into the trends and patterns shaping the world's energy landscape.
                    From fluctuations in demand to the impact of renewable sources, our statistics provide a nuanced perspective on the complexities of energy consumption.
                </p>
                <img className='mt-10' src="/assets/energy.png" alt="" srcset="" />
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
            <div className='p-5 md:p-20 col-span-full w-full bg-white'>
                <h3 className='text-blue-600 font-graphik  font-bold text-3xl md:text-4xl mb-8'>Consumption over Years {' '}
                    <span className='text-slate-400 text-lg '>
                        (mtoe)
                    </span>
                </h3>

                <BarChartGraph data={barData} type="consumption" />
            </div>


        </div>
    )
}

export default Consumption;