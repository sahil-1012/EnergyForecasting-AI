import Tabs from '@mui/material/Tabs';
import Tab from '@mui/material/Tab';
import Consumption from './Consumption';
import { useState } from 'react';
import Production from './Production';

export default function Menu() {
    const [value, setValue] = useState(0);

    const handleChange = (event, newValue) => {
        setValue(newValue);
    };

    return (
        <>
            <div className="border-b border-divider px-16 ">
                <Tabs TabIndicatorProps={{ sx: { backgroundColor: 'black' } }} value={value} onChange={handleChange}>
                    <Tab
                        label="Consumption"
                        sx={{
                            width: '150px',
                            color: 'gray',
                            '&.Mui-selected': {
                                color: 'black',
                                fontWeight: 'bold',
                            },
                            paddingBottom: '24px',
                            fontSize: '16px',
                            fontFamily: 'Poppins, sans-serif',
                            textTransform: 'none',
                        }}
                        disableRipple
                    />

                    <Tab
                        label="Production"
                        sx={{
                            width: '150px',
                            color: 'gray',
                            '&.Mui-selected': {
                                color: 'black',
                                fontWeight: 'bold',
                            },
                            paddingBottom: '24px',
                            fontSize: '16px',
                            fontFamily: 'Poppins, sans-serif',
                            textTransform: 'none',
                        }}
                        disableRipple
                    />

                    <Tab
                        label="Prediction"
                        sx={{
                            color: 'gray',
                            width: '150px',
                            '&.Mui-selected': {
                                color: 'black',
                                fontWeight: 'bold',
                            },
                            paddingBottom: '24px',
                            fontSize: '16px',
                            fontFamily: 'Poppins, sans-serif',
                            textTransform: 'none',
                        }}
                        disableRipple
                    />
                    <Tab
                        label="World"
                        sx={{
                            color: 'gray',
                            width: '150px',
                            '&.Mui-selected': {
                                color: 'black',
                                fontWeight: 'bold',
                            },
                            paddingBottom: '24px',
                            fontSize: '16px',
                            fontFamily: 'Poppins, sans-serif',
                            textTransform: 'none',
                        }}
                        disableRipple
                    />
                    <Tab
                        label="About Us"
                        sx={{
                            color: 'gray',
                            width: '150px',
                            '&.Mui-selected': {
                                color: 'black',
                                fontWeight: 'bold',
                            },
                            paddingBottom: '24px',
                            fontSize: '16px',
                            fontFamily: 'Poppins, sans-serif',
                            textTransform: 'none',
                        }}
                        disableRipple
                    />
                </Tabs>

            </div>
            {value === 0 && <Consumption value={value} />}
            {value === 1 && <Production value={value} />}
            {/* {value === 3 && <Consumption value={value} />} */}
        </>
    );
}
