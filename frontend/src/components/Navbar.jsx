import React from 'react';
import IconButton from '@mui/material/IconButton';
import SearchIcon from '@mui/icons-material/Search';

const Navbar = () => {

    return (
        <div className="flex h-full items-center md:justify-between py-2 px-6 border-b-2 sticky top-0 z-50 bg-white">
            {/* <IconButton
                size="small"
                edge="start"
                color="primary"
                aria-label="open drawer"
                className="flex md:hidden"
            >
                <MenuIcon />
            </IconButton> */}

            <h4 className="flex-grow text-blue-600 font-bold text-3xl w-full">
                ЭIS <span className="text-base font-normal  text-gray-400">Energize, Innovate, Succeed</span>
            </h4>


            <div className="w-96 h-10 hidden md:flex bg-gray-100 px-3 py-0.5 items-center justify-between rounded-lg focus-within:bg-transparent focus-within:border focus-within:border-gray-300">
                <input
                    className="mx-2 w-full bg-transparent outline-none border-none"
                    placeholder="Search"
                />

                <IconButton size="small">
                    <SearchIcon />
                </IconButton>
            </div>
        </div>
    );
};

export default Navbar;
