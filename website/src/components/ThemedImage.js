import React from 'react';
import ThemedImage from '@theme/ThemedImage';

export default ({light, dark, alt}) => {
    return <ThemedImage alt={alt} sources={{
        light: require(`../../docs/assets/${light}`).default,
        dark: require(`../../docs/assets/${dark}`).default
    }}/>;
};