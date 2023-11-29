import React from 'react'

const Header = () => {
    return (
        <header className="h-full flex justify-end">
            <div className='w-full relative text-gray-900'>
                <h1 className="mt-24 mb-16 md:mt-36 md:mb-24 mx-7 min-[550px]:mx-16 font-graphik text-5xl sm:6xl md:text-7xl lg:text-8xl font-bold ">
                    Data and {' '}
                    <span className='inline-block lg:block' >
                        statistics
                    </span>
                </h1>
            </div>

            <div className='w-full hidden lg:inline-block'>
                <h3 className="text-blue-600 font-graphik text-xl font-semibold transform -rotate-90 w-44 mt-48 ml-auto mr-10">
                    International <br /> Energy Insights
                </h3>
            </div>
        </header>
    )
}

export default Header