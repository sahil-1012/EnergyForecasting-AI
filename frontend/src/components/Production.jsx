import React, { useEffect, useState } from 'react'
import { PieChartGraph } from '../charts/PieChartGraph';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select from '@mui/material/Select';
import BarChartGraph from '../charts/BarChartGraph';


const Production = ({ value }) => {
    const [data, setData] = useState();
    const [barData, setBarData] = useState();
    const [loading, setLoading] = useState(true);
    const [year, setYear] = useState(2022);
    const [productionType, setProductionType] = useState('total');

    useEffect(() => {
        const fetchData = async () => {
            try {
                const response = await fetch(`http://localhost:4000/api/getTotalProduction?year=${year}`);

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
    }, [year, productionType]);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const response = await fetch('http://localhost:4000/api/getTotalProductionOverYears');
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
                <h2 className='text-blue-600 font-graphik  font-bold text-3xl md:text-4xl'>Energy Production Statistics</h2>
                <p class="text-slate-500 my-4 w-full">
                    Navigate through the extensive selection of EIS statistics with our user-friendly data browser. 
                    Explore charts and tables covering 16 energy topics for over 170 countries and regions. 
                    Whether you're tracking global trends or focusing on specific regions, our data browser provides a seamless experience for accessing vital energy statistics.
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
                                value={productionType}
                                label="Type"
                                onChange={(e) => setProductionType(e.target.value)}
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
            <div className='p-5 md:p-20 col-span-full w-full '>
                <h3 className='text-blue-600 font-graphik  font-bold text-3xl md:text-4xl mb-8'>Production over Years {' '}
                    <span className='text-slate-400 text-lg '>
                        (mtoe)
                    </span>
                </h3>

                <BarChartGraph data={barData} type="production" />
            </div>


        </div>
    )
}

export default Production;