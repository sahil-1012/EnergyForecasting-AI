import React, { useEffect, useState } from 'react'
import LineGraph from '../charts/LineChartGraph'

const CarbonEmission = () => {
    const [carbonEmission, setCarbonEmission] = useState(null);

    useEffect(() => {
        const fetchData = async () => {
            try {
                const response = await fetch('http://localhost:4000/api/getCarbonEmission');
                const data = await response.json();
                setCarbonEmission(data);
            } catch (error) {
                console.error('Error fetching carbon emission data:', error);
            }
        };

        fetchData();
    }, []);

    return (
        <div className="bg-blue-50 h-full grid grid-cols-2">
            <div className='p-5 md:p-20 col-span-full w-full '>
                <h3 className='text-blue-600 font-graphik font-bold text-xl md:text-3xl ml-10'>Carbon Emission over Years</h3>
                <p className='text-slate-400 text-base mt-3 mb-16 ml-10'>Despite the diligent efforts in raising awareness about sustainable practices, the data reveals that carbon emissions have not seen a significant decrease. This concerning trend poses challenges for our global community, emphasizing the persistent need for sustainable energy solutions. Our platform remains committed to providing valuable insights into these trends, empowering individuals and organizations to make informed decisions for a greener future.</p>
                <LineGraph data={carbonEmission} dataKey={'year'} />
            </div>
        </div>
    )
}

export default CarbonEmission